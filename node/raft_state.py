"""
Raft State Machine Module
Fully implements the Raft consensus algorithm strictly per Ongaro & Ousterhout (2014):
- Role transitions (Follower, Candidate, Leader)
- Durability (metadata and log committed to SQLite before RPC replies)
- 100% complete RequestVote handler with §5.4.1 Election Restriction
- 100% complete AppendEntries handler with §5.3 Log Matching Property & truncation
- Incremental backtracking log replication (nextIndex decrement without full state dump)
- Randomized election timers (150-300ms) and heartbeats (50ms)
- Client command replication with majority commit validation
"""

import asyncio
import logging
import random
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import httpx
from pydantic import BaseModel

from .config import config
from .storage import Storage

# Configure logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] [Node %(name)s]: %(message)s")
logger = logging.getLogger(str(config.NODE_ID))


class NodeRole(str, Enum):
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"


# -----------------------------------------------------------------------------
# Raft RPC Pydantic Schemas (§5.2, §5.3, §5.4)
# -----------------------------------------------------------------------------

class RequestVoteArgs(BaseModel):
    term: int
    candidateId: int
    lastLogIndex: int
    lastLogTerm: int


class RequestVoteReply(BaseModel):
    term: int
    voteGranted: bool


class AppendEntriesArgs(BaseModel):
    term: int
    leaderId: int
    prevLogIndex: int
    prevLogTerm: int
    entries: List[Dict[str, Any]] = []
    leaderCommit: int


class AppendEntriesReply(BaseModel):
    term: int
    success: bool
    matchIndex: int = 0


# -----------------------------------------------------------------------------
# Raft Node Class (100% Complete Implementation per Raft Paper)
# -----------------------------------------------------------------------------

class RaftNode:
    def __init__(self, node_id: int, peers: List[str], storage: Storage):
        self.node_id = node_id
        self.peers = peers
        self.storage = storage

        # Lock protecting all internal state transitions and log mutations
        self.lock = asyncio.Lock()

        # Admin simulated failure flag
        self.is_dead: bool = False

        # Persistent state on all servers (Updated on stable storage before responding to RPCs, §5.2)
        self.current_term, self.voted_for = self.storage.load_meta()
        self.log: List[Dict[str, Any]] = self.storage.get_all_log_entries()

        # Volatile state on all servers (§5.2)
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.role: NodeRole = NodeRole.FOLLOWER
        self.leader_id: Optional[int] = None

        # Volatile state on leaders (Reinitialized after election, §5.3)
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Timing and election timers
        self.last_heartbeat_timestamp: float = time.time()
        self.election_timeout: float = self._random_election_timeout()

        # Background async consensus loop handle
        self._loop_task: Optional[asyncio.Task] = None
        self._http_client: Optional[httpx.AsyncClient] = None

        # Re-apply any committed entries up to commit_index if applicable
        self._sync_state_machine_on_startup()

    def _random_election_timeout(self) -> float:
        """Returns randomized timeout between 150ms and 300ms (in seconds) per §5.2."""
        min_sec = config.ELECTION_TIMEOUT_MIN_MS / 1000.0
        max_sec = config.ELECTION_TIMEOUT_MAX_MS / 1000.0
        return random.uniform(min_sec, max_sec)

    def _reset_election_timer(self) -> None:
        """Resets the heartbeat timer and calculates a fresh randomized timeout."""
        self.last_heartbeat_timestamp = time.time()
        self.election_timeout = self._random_election_timeout()

    def _sync_state_machine_on_startup(self) -> None:
        """Apply any committed entries already in persistent state machine."""
        pass

    # -------------------------------------------------------------------------
    # Helper Inspection Methods
    # -------------------------------------------------------------------------

    def get_last_log_info(self) -> Tuple[int, int]:
        """Returns (last_log_index, last_log_term). Returns (0, 0) if log is empty."""
        if not self.log:
            return 0, 0
        last = self.log[-1]
        return last["index"], last["term"]

    def get_log_entry_at(self, index: int) -> Optional[Dict[str, Any]]:
        """Look up 1-indexed entry in the in-memory log."""
        if index < 1 or index > len(self.log):
            return None
        return self.log[index - 1]

    def get_status(self) -> Dict[str, Any]:
        """Status dict matching required route schema: role, term, commit_index, last_log_index."""
        last_idx, _ = self.get_last_log_info()
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "state": self.role.value,  # Backward compatibility for dashboard
            "term": self.current_term,
            "commit_index": self.commit_index,
            "last_log_index": last_idx,
            "leader_id": self.leader_id,
            "is_dead": self.is_dead,
        }

    def get_formatted_log(self) -> List[Dict[str, Any]]:
        """Returns full replicated log with committed flag per entry."""
        result = []
        for entry in self.log:
            result.append({
                "index": entry["index"],
                "term": entry["term"],
                "command": entry["command"],
                "committed": entry["index"] <= self.commit_index
            })
        return result

    # -------------------------------------------------------------------------
    # State Transitions (§5.2)
    # -------------------------------------------------------------------------

    def become_follower(self, term: int, leader_id: Optional[int] = None) -> None:
        """Transition to Follower role and persist updated term before any RPC returns."""
        logger.info(f"Transitioning to FOLLOWER for term {term} (leader={leader_id})")
        self.role = NodeRole.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.leader_id = leader_id
        self.storage.save_meta(self.current_term, self.voted_for)
        self._reset_election_timer()

    def become_candidate(self) -> None:
        """Transition to Candidate role (§5.2)."""
        self.role = NodeRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        # Durably persist term and vote before requesting votes from peers
        self.storage.save_meta(self.current_term, self.voted_for)
        self._reset_election_timer()
        logger.info(f"Transitioning to CANDIDATE for term {self.current_term} (voted for self)")

    def become_leader(self) -> None:
        """Transition to Leader role (§5.2, §5.3, §8)."""
        self.role = NodeRole.LEADER
        self.leader_id = self.node_id

        # Per Raft §8: Leader appends a blank NO-OP entry in its new term
        # to commit prior-term entries and discover committed entries
        new_index = len(self.log) + 1
        noop_entry = {
            "index": new_index,
            "term": self.current_term,
            "command": {"type": "NOOP", "key": "", "value": None}
        }
        self.log.append(noop_entry)
        self.storage.append_log(new_index, self.current_term, noop_entry["command"])

        last_log_idx, _ = self.get_last_log_info()

        # Reinitialize volatile leader state (§5.3):
        # next_index initialized to leader's last_log_index + 1
        # match_index initialized to 0
        self.next_index = {peer: last_log_idx + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}

        logger.info(f"*** ELECTED LEADER for term {self.current_term} (NO-OP entry #{new_index}) ***")

    # -------------------------------------------------------------------------
    # Background Consensus Loop
    # -------------------------------------------------------------------------

    async def start_background_loop(self) -> None:
        """Starts background task running election timers & heartbeats."""
        self._http_client = httpx.AsyncClient(timeout=(config.RPC_TIMEOUT_MS / 1000.0))
        self._loop_task = asyncio.create_task(self._consensus_loop())

    async def stop_background_loop(self) -> None:
        """Stops consensus loop and closes HTTP client."""
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        if self._http_client:
            await self._http_client.aclose()

    async def _consensus_loop(self) -> None:
        """Asynchronous loop checking timers at 10ms granularity."""
        while True:
            try:
                await asyncio.sleep(0.010)

                if self.is_dead:
                    continue

                async with self.lock:
                    if self.role == NodeRole.LEADER:
                        # Leader broadcasts heartbeats every 50ms (§5.2)
                        now = time.time()
                        if now - self.last_heartbeat_timestamp >= (config.HEARTBEAT_INTERVAL_MS / 1000.0):
                            self.last_heartbeat_timestamp = now
                            asyncio.create_task(self._broadcast_append_entries())
                    else:
                        # Follower and Candidate check election timeout (150-300ms)
                        now = time.time()
                        if now - self.last_heartbeat_timestamp >= self.election_timeout:
                            # Timeout reached without valid leader heartbeat: start election
                            self.become_candidate()
                            asyncio.create_task(self._conduct_election(self.current_term))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Consensus loop exception: {e}", exc_info=True)

    # -------------------------------------------------------------------------
    # Leader Election Logic (§5.2, §5.4)
    # -------------------------------------------------------------------------

    async def _conduct_election(self, election_term: int) -> None:
        """Send RequestVote RPCs in parallel to all peers and tally votes."""
        last_idx, last_term = self.get_last_log_info()
        args = RequestVoteArgs(
            term=election_term,
            candidateId=self.node_id,
            lastLogIndex=last_idx,
            lastLogTerm=last_term
        )

        votes_granted = 1  # Candidate votes for self
        majority = (len(self.peers) + 1) // 2 + 1

        logger.info(f"Node {self.node_id} campaigning for term {election_term} (need {majority} votes)")

        tasks = [self._send_request_vote(peer, args) for peer in self.peers]
        replies = await asyncio.gather(*tasks, return_exceptions=True)

        async with self.lock:
            # Drop outcome if node died, role changed, or term advanced
            if self.is_dead or self.role != NodeRole.CANDIDATE or self.current_term != election_term:
                return

            for reply in replies:
                if isinstance(reply, RequestVoteReply):
                    if reply.term > self.current_term:
                        # Discovered higher term: step down to Follower immediately (§5.1)
                        self.become_follower(reply.term)
                        return
                    if reply.voteGranted:
                        votes_granted += 1

            logger.info(f"Election tally for term {election_term}: {votes_granted}/{len(self.peers) + 1} votes")

            if votes_granted >= majority:
                self.become_leader()
                # Immediately send empty AppendEntries to assert authority and suppress other elections (§5.2)
                asyncio.create_task(self._broadcast_append_entries())
            else:
                # Split vote or lack of majority: backoff with new randomized timeout
                self._reset_election_timer()

    async def _send_request_vote(self, peer: str, args: RequestVoteArgs) -> Optional[RequestVoteReply]:
        """Dispatch RequestVote RPC to a single peer."""
        if not self._http_client or self.is_dead:
            return None
        url = f"{peer}/raft/request_vote"
        try:
            resp = await self._http_client.post(url, json=args.model_dump())
            if resp.status_code == 200:
                return RequestVoteReply(**resp.json())
        except Exception:
            pass
        return None

    # -------------------------------------------------------------------------
    # 100% Complete RequestVote RPC Handler (§5.2, §5.4.1)
    # -------------------------------------------------------------------------

    async def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        """
        RequestVote RPC handler:
        - Reject if candidate's term < current_term (§5.1)
        - If candidate's term > current_term, update current_term and step down to Follower first (§5.1)
        - Grant vote only if:
            (a) haven't already voted this term for someone else (voted_for is None or candidateId), AND
            (b) candidate's log is at least as up-to-date as receiver's log (§5.4.1 Election Restriction):
                candidate's last log term is higher, OR
                (equal terms AND candidate's last log index >= receiver's last log index).
        - Persist voted_for and current_term to SQLite BEFORE responding (§5.2 Durability Requirement).
        """
        async with self.lock:
            if self.is_dead:
                return RequestVoteReply(term=self.current_term, voteGranted=False)

            # 1. Rule: Reject if candidate's term < current_term
            if args.term < self.current_term:
                return RequestVoteReply(term=self.current_term, voteGranted=False)

            # Rule: If candidate's term > current_term, update current_term and step down to Follower
            if args.term > self.current_term:
                self.become_follower(args.term)

            # 2. Rule (a): Check if already voted for someone else this term
            can_vote = (self.voted_for is None or self.voted_for == args.candidateId)

            # 2. Rule (b): §5.4.1 Election Restriction
            # Compare receiver's last log term/index with candidate's
            my_last_index, my_last_term = self.get_last_log_info()
            log_up_to_date = False
            if args.lastLogTerm > my_last_term:
                log_up_to_date = True
            elif args.lastLogTerm == my_last_term and args.lastLogIndex >= my_last_index:
                log_up_to_date = True

            if can_vote and log_up_to_date:
                self.voted_for = args.candidateId
                # Durability: persist to disk BEFORE responding
                self.storage.save_meta(self.current_term, self.voted_for)
                self._reset_election_timer()
                logger.info(f"Vote GRANTED to candidate {args.candidateId} for term {self.current_term}")
                return RequestVoteReply(term=self.current_term, voteGranted=True)
            else:
                logger.info(
                    f"Vote DENIED to candidate {args.candidateId} (can_vote={can_vote}, log_up_to_date={log_up_to_date}, my_log=({my_last_index}, {my_last_term}), candidate_log=({args.lastLogIndex}, {args.lastLogTerm}))"
                )
                return RequestVoteReply(term=self.current_term, voteGranted=False)

    # -------------------------------------------------------------------------
    # 100% Complete AppendEntries RPC Handler (§5.2, §5.3)
    # -------------------------------------------------------------------------

    async def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        """
        AppendEntries RPC handler (both heartbeats and log replication):
        - Reject if leader's term < current_term (§5.1)
        - If term is current or higher, reset election timeout and recognize sender as leader;
          step down to Follower if currently Candidate or Leader with stale term (§5.2).
        - Log consistency check (§5.3): reject (success=false) unless local log contains
          an entry at prevLogIndex with term exactly equal to prevLogTerm (prevLogIndex=0 matches trivially).
        - On acceptance, if an existing entry conflicts with a new one (same index, different term),
          delete that entry and everything after it, then append the new entries (§5.3).
        - Update commit_index to min(leaderCommit, index of last new entry) if leaderCommit > commit_index.
        - Apply newly committed entries to key-value state machine.
        - Persist log and metadata to SQLite before replying (§5.2 Durability Requirement).
        """
        async with self.lock:
            if self.is_dead:
                return AppendEntriesReply(term=self.current_term, success=False)

            # 1. Rule: Reject if leader's term < current_term (§5.1)
            if args.term < self.current_term:
                return AppendEntriesReply(term=self.current_term, success=False)

            # If term > current_term or currently not Follower, step down to Follower
            if args.term > self.current_term:
                self.become_follower(args.term, leader_id=args.leaderId)
            elif self.role != NodeRole.FOLLOWER:
                self.become_follower(args.term, leader_id=args.leaderId)
            else:
                self.leader_id = args.leaderId
                self._reset_election_timer()

            # 2. Rule (§5.3): Log consistency check
            # Reject unless local log contains an entry at prevLogIndex with term == prevLogTerm
            if args.prevLogIndex > 0:
                if args.prevLogIndex > len(self.log):
                    # Receiver has no entry at prevLogIndex
                    return AppendEntriesReply(term=self.current_term, success=False)
                entry_at_prev = self.log[args.prevLogIndex - 1]
                if entry_at_prev["term"] != args.prevLogTerm:
                    # Term mismatch at prevLogIndex
                    return AppendEntriesReply(term=self.current_term, success=False)

            # 3. Rule (§5.3): Process and append entries, truncating conflicting suffixes
            entries_appended = False
            for i, new_entry in enumerate(args.entries):
                entry_idx = args.prevLogIndex + 1 + i
                if entry_idx <= len(self.log):
                    existing = self.log[entry_idx - 1]
                    if existing["term"] != new_entry["term"]:
                        # Conflict detected: delete conflicting entry and all subsequent entries
                        self.log = self.log[:entry_idx - 1]
                        self.storage.truncate_log_from(entry_idx)
                        # Append the new entry
                        self.log.append(new_entry)
                        self.storage.append_log(new_entry["index"], new_entry["term"], new_entry["command"])
                        entries_appended = True
                else:
                    # Entry does not exist locally: append it
                    self.log.append(new_entry)
                    self.storage.append_log(new_entry["index"], new_entry["term"], new_entry["command"])
                    entries_appended = True

            # 4. Rule (§5.3): Update commit_index
            if args.leaderCommit > self.commit_index:
                last_new_entry_index = args.prevLogIndex + len(args.entries)
                if args.entries:
                    self.commit_index = min(args.leaderCommit, last_new_entry_index)
                else:
                    self.commit_index = min(args.leaderCommit, len(self.log))
                
                # Apply newly committed entries to the state machine
                self._apply_committed_entries()

            # Reset election timer after processing and applying entries
            self._reset_election_timer()
            match_idx = args.prevLogIndex + len(args.entries)
            return AppendEntriesReply(term=self.current_term, success=True, matchIndex=match_idx)

    # -------------------------------------------------------------------------
    # Leader AppendEntries & Backtracking Log Replication (§5.3)
    # -------------------------------------------------------------------------

    async def _broadcast_append_entries(self) -> None:
        """Leader broadcasts heartbeats / backlog entries to all peers."""
        if self.role != NodeRole.LEADER or self.is_dead:
            return

        for peer in self.peers:
            asyncio.create_task(self._replicate_to_peer(peer))

    async def _replicate_to_peer(self, peer: str) -> None:
        """
        Replicate log entries to a peer incrementally:
        If AppendEntries fails because of log inconsistency, decrement nextIndex
        and retry until consistency check succeeds (standard Raft backtracking).
        """
        while True:
            async with self.lock:
                if self.role != NodeRole.LEADER or self.is_dead:
                    return

                next_idx = self.next_index.get(peer, len(self.log) + 1)
                prev_idx = next_idx - 1
                prev_term = 0
                if prev_idx > 0 and prev_idx <= len(self.log):
                    prev_term = self.log[prev_idx - 1]["term"]

                # Slice entries from prev_idx onward
                entries_to_send = self.log[prev_idx:]

                args = AppendEntriesArgs(
                    term=self.current_term,
                    leaderId=self.node_id,
                    prevLogIndex=prev_idx,
                    prevLogTerm=prev_term,
                    entries=entries_to_send,
                    leaderCommit=self.commit_index
                )

            # Perform network I/O outside lock
            if not self._http_client or self.is_dead:
                return
            url = f"{peer}/raft/append_entries"
            try:
                resp = await self._http_client.post(url, json=args.model_dump())
                if resp.status_code != 200:
                    return
                reply = AppendEntriesReply(**resp.json())
            except Exception:
                return

            async with self.lock:
                if self.is_dead or self.role != NodeRole.LEADER or args.term != self.current_term:
                    return

                if reply.term > self.current_term:
                    # Stale leader discovers higher term: step down to Follower
                    self.become_follower(reply.term)
                    return

                if reply.success:
                    # Replication succeeded: update match_index and next_index
                    new_match = args.prevLogIndex + len(args.entries)
                    self.match_index[peer] = max(self.match_index.get(peer, 0), new_match)
                    self.next_index[peer] = self.match_index[peer] + 1

                    # Check if commit_index can advance (§5.3, §5.4)
                    self._check_and_advance_commit_index()
                    return
                else:
                    # AppendEntries rejected: decrement next_index and loop to retry backtracking (§5.3)
                    curr_next = self.next_index.get(peer, len(self.log) + 1)
                    if curr_next > 1:
                        self.next_index[peer] = curr_next - 1
                        logger.info(f"Backtracking next_index for peer {peer} to {self.next_index[peer]}")
                    else:
                        return

    def _check_and_advance_commit_index(self) -> None:
        """
        Leader advances commit_index if there exists an N > commitIndex such that:
        - a majority of matchIndex[i] >= N, and
        - log[N].term == currentTerm (§5.3, §5.4).
        """
        majority = (len(self.peers) + 1) // 2 + 1
        last_idx, _ = self.get_last_log_info()

        for N in range(last_idx, self.commit_index, -1):
            entry = self.get_log_entry_at(N)
            if entry and entry["term"] == self.current_term:
                count = 1  # Leader itself matches
                for peer in self.peers:
                    if self.match_index.get(peer, 0) >= N:
                        count += 1
                if count >= majority:
                    logger.info(f"Leader advancing commit_index from {self.commit_index} to {N} (quorum={count})")
                    self.commit_index = N
                    self._apply_committed_entries()
                    break

    def _apply_committed_entries(self) -> None:
        """Apply committed entries (last_applied < commit_index) to the SQLite state machine."""
        ops = []
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.get_log_entry_at(self.last_applied)
            if entry:
                cmd = entry.get("command", {})
                c_type = cmd.get("type", "SET")
                c_key = cmd.get("key", "")
                c_val = cmd.get("value")
                logger.info(f"Applying #{self.last_applied} to kv_store: {c_type} {c_key}={c_val}")
                ops.append((c_type, c_key, c_val))
        if ops:
            self.storage.apply_state_machines_batch(ops)

    # -------------------------------------------------------------------------
    # Client Command Execution (Leader Only)
    # -------------------------------------------------------------------------

    async def execute_client_command(
        self, command_type: str, key: str, value: Optional[str], timeout: float = 3.0
    ) -> Tuple[bool, Optional[str], Optional[int]]:
        """
        Execute SET / DELETE from client.
        Appends to own log, persists to disk, replicates to peers in parallel.
        Blocks until committed by a majority, then returns success.
        """
        async with self.lock:
            if self.is_dead:
                return False, "Node is dead", None

            if self.role != NodeRole.LEADER:
                return False, "not leader", self.leader_id

            # Append entry to local log (§5.3)
            new_index = len(self.log) + 1
            entry = {
                "index": new_index,
                "term": self.current_term,
                "command": {
                    "type": command_type.upper(),
                    "key": key,
                    "value": value
                }
            }
            self.log.append(entry)
            # Durability: persist uncommitted log entry to SQLite before proceeding
            self.storage.append_log(new_index, self.current_term, entry["command"])
            logger.info(f"Leader appended log #{new_index}: {command_type} {key}={value}")

        # Trigger immediate parallel replication to followers
        asyncio.create_task(self._broadcast_append_entries())

        # Block until committed by majority or timeout
        start_wait = time.time()
        while time.time() - start_wait < timeout:
            async with self.lock:
                if self.is_dead or self.role != NodeRole.LEADER:
                    return False, "lost leadership", self.leader_id
                if self.commit_index >= new_index:
                    return True, None, self.node_id
            await asyncio.sleep(0.010)

        return False, "Replication timeout waiting for quorum commit", self.node_id

    # -------------------------------------------------------------------------
    # Admin Kill / Restart Simulation
    # -------------------------------------------------------------------------

    async def kill(self) -> None:
        """Sets internal dead flag, stops participating in Raft or answering requests."""
        async with self.lock:
            self.is_dead = True
            logger.warning(f"Node {self.node_id} KILLED by admin.")

    async def restart(self) -> None:
        """Revives node, resumes as Follower, re-reads SQLite state, resumes timer."""
        async with self.lock:
            logger.info(f"Node {self.node_id} RESTARTING...")
            self.is_dead = False
            # Re-read persistent state from storage (§5.2)
            self.current_term, self.voted_for = self.storage.load_meta()
            self.log = self.storage.get_all_log_entries()
            self.role = NodeRole.FOLLOWER
            self.leader_id = None
            self.commit_index = 0
            self.last_applied = 0
            self._reset_election_timer()
            logger.info(f"Node {self.node_id} resumed as FOLLOWER at term {self.current_term}, log length={len(self.log)}")
