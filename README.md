# 3-Node Distributed Key-Value Store with Raft Consensus

A complete distributed Key-Value store built with **Python**, **FastAPI**, **SQLite**, and an interactive **Single-Page Dashboard** in Vanilla HTML/CSS/JS.

Built for university distributed systems coursework to explore and experiment with Raft consensus, leader election, log replication, failure recovery, and client redirection.

---

## 📁 Project Structure

```
.
├── node/
│   ├── __init__.py
│   ├── config.py         # Node configuration (ports, peers, election timeouts)
│   ├── storage.py        # SQLite persistence for term, voted_for, log, and KV state machine
│   ├── raft_state.py     # Raft consensus state machine & educational safety TODOs
│   └── main.py           # FastAPI server with KV routes, Raft RPCs, and admin kill/restart
├── frontend/
│   └── index.html        # Single-file live observation and control dashboard
├── run_cluster.py        # Python multi-process cluster launcher (Node 1, 2, 3)
├── run_cluster.ps1       # PowerShell launcher script
├── run_cluster.bat       # Windows Batch launcher script
└── README.md
```

---

## 🚀 Quick Start

### 1. Requirements
- Python 3.10+ (installed in `.venv` with `fastapi`, `uvicorn`, `httpx`, `pydantic`).

### 2. Launching the 3-Node Cluster
Run the provided launcher in the project root:

```bash
# Using python:
python run_cluster.py

# Or in PowerShell:
.\run_cluster.ps1

# Or double-click:
run_cluster.bat
```

This starts 3 nodes on `localhost`:
- **Node 1**: `http://127.0.0.1:8001`
- **Node 2**: `http://127.0.0.1:8002`
- **Node 3**: `http://127.0.0.1:8003`

### 3. Open Dashboard
Open `frontend/index.html` in any modern web browser (Edge, Chrome, Firefox). No build step, node_modules, or web server needed.

---

## 🎓 Academic Assignment Notes: Raft Safety Checks

Per assignment requirements, the node compiles and runs so your team has a working cluster to test immediately. However, **two core Raft safety checks** have been flagged with prominent docstrings and `# TODO` markers inside [`node/raft_state.py`](node/raft_state.py):

### 1. §5.4.1 Election Restriction (`check_log_up_to_date`)
- **Location**: `RaftNode.check_log_up_to_date` in [`node/raft_state.py`](node/raft_state.py)
- **Paper Requirement**: Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries:
  1. If the logs have last entries with different terms, the log with the later term is more up-to-date.
  2. If the logs end with the same term, whichever log has more entries (higher index) is more up-to-date.
  3. A follower **must deny its vote** if the candidate's log is less up-to-date than its own log.

### 2. §5.3 Log Matching Property (`check_log_consistency`)
- **Location**: `RaftNode.check_log_consistency` in [`node/raft_state.py`](node/raft_state.py)
- **Paper Requirement**: When receiving `AppendEntries`:
  1. If `prevLogIndex == 0`, accept trivially.
  2. If receiver has no entry at `prevLogIndex` (`prevLogIndex > last_log_index`), reject with `success = False`.
  3. If receiver has an entry at `prevLogIndex` but its `term != prevLogTerm`, reject with `success = False` to force leader to decrement `next_index` and find common ancestor.
  4. If an existing entry conflicts with a new one (same index, different term), truncate all subsequent entries.

---

## 📡 API Endpoints

### Client-Facing KV Routes
- `GET /status`: Returns node state (`Leader`, `Candidate`, `Follower`), term, commit index, last log index.
- `GET /log`: Returns full replicated log with committed flags.
- `GET /kv/get?key=KEY`: Returns `{key, value}` or 404.
- `POST /kv/set`: Body `{key, value}`. If called on a follower, returns `HTTP 421 Misdirected Request` with `{error: "not leader", leader_hint: ID}`.
- `POST /kv/delete`: Body `{key}`. Returns 421 if not leader.

### Inter-Node Raft RPC Routes
- `POST /raft/request_vote`: Candidate gathers votes during election (§5.2, §5.4).
- `POST /raft/append_entries`: Leader sends heartbeats (every 50ms) and log entries (§5.2, §5.3).

### Admin / Testing Routes
- `POST /admin/kill`: Sets internal `is_dead = True`. Simulates a node crash (all requests return 503, no Raft RPCs sent/received).
- `POST /admin/restart`: Sets `is_dead = False`. Node revives as a `Follower`, re-reads persisted SQLite state, and catches up.
