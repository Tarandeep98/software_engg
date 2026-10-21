"""
Storage Module for Raft Node
Implements persistent storage using SQLite with exact tables:
1. raft_meta: single row (current_term, voted_for), updated in place
2. raft_log: log entries (log_index, term, command as JSON string)
3. kv_store: committed state machine table (key, value)

All Raft metadata and log writes are explicitly committed/fsync'd to disk
before the corresponding RPC response is returned.
"""

import json
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        # Single SQLite connection reused across all methods
        self.conn = sqlite3.connect(self.db_path, timeout=15.0, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=FULL;")
        self._init_db()

    def _init_db(self) -> None:
        """Initialize SQLite database with the 3 required tables."""
        with self._lock:
            cursor = self.conn.cursor()
            
            # 1. raft_meta (current_term, voted_for - single row, updated in place)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raft_meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_term INTEGER NOT NULL,
                    voted_for INTEGER
                );
            """)
            
            # 2. raft_log (log_index, term, command as JSON)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raft_log (
                    log_index INTEGER PRIMARY KEY,
                    term INTEGER NOT NULL,
                    command TEXT NOT NULL
                );
            """)
            
            # 3. kv_store (key, value)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kv_store (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)
            
            # Insert initial single row for metadata if not present
            cursor.execute("SELECT id FROM raft_meta WHERE id = 1;")
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO raft_meta (id, current_term, voted_for) VALUES (1, 0, NULL);"
                )
            
            self.conn.commit()

    # -------------------------------------------------------------------------
    # Metadata Operations (§5.2)
    # -------------------------------------------------------------------------

    def load_meta(self) -> Tuple[int, Optional[int]]:
        """Load current_term and voted_for from raft_meta table."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT current_term, voted_for FROM raft_meta WHERE id = 1;")
            row = cursor.fetchone()
            if row:
                return row["current_term"], row["voted_for"]
            return 0, None

    def save_meta(self, current_term: int, voted_for: Optional[int]) -> None:
        """Persist current_term and voted_for to raft_meta with fsync before returning."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE raft_meta SET current_term = ?, voted_for = ? WHERE id = 1;",
                (current_term, voted_for)
            )
            self.conn.commit()

    # -------------------------------------------------------------------------
    # Log Operations (§5.3)
    # -------------------------------------------------------------------------

    def append_log(self, log_index: int, term: int, command: Dict[str, Any]) -> None:
        """Append or replace a single log entry, committing to disk."""
        with self._lock:
            cursor = self.conn.cursor()
            cmd_json = json.dumps(command)
            cursor.execute("""
                INSERT OR REPLACE INTO raft_log (log_index, term, command)
                VALUES (?, ?, ?);
            """, (log_index, term, cmd_json))
            self.conn.commit()

    def append_logs(self, entries: List[Dict[str, Any]]) -> None:
        """Batch append log entries with a single durable transaction."""
        if not entries:
            return
        with self._lock:
            cursor = self.conn.cursor()
            for e in entries:
                cmd_json = json.dumps(e.get("command", {}))
                cursor.execute("""
                    INSERT OR REPLACE INTO raft_log (log_index, term, command)
                    VALUES (?, ?, ?);
                """, (e["index"], e["term"], cmd_json))
            self.conn.commit()

    def truncate_log_from(self, from_index: int) -> None:
        """Delete all log entries starting from `from_index` onward."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM raft_log WHERE log_index >= ?;", (from_index,))
            self.conn.commit()

    def get_all_log_entries(self) -> List[Dict[str, Any]]:
        """Fetch all log entries ordered by log_index ascending."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT log_index, term, command FROM raft_log ORDER BY log_index ASC;")
            rows = cursor.fetchall()
            entries = []
            for r in rows:
                entries.append({
                    "index": r["log_index"],
                    "term": r["term"],
                    "command": json.loads(r["command"])
                })
            return entries

    # -------------------------------------------------------------------------
    # State Machine Operations (kv_store table)
    # -------------------------------------------------------------------------

    def apply_state_machines_batch(self, operations: List[Tuple[str, str, Optional[str]]]) -> None:
        """Apply a batch of committed commands to the kv_store in a single transaction."""
        if not operations:
            return
        with self._lock:
            cursor = self.conn.cursor()
            for command_type, key, value in operations:
                if command_type.upper() == "NOOP":
                    continue
                elif command_type.upper() == "SET":
                    cursor.execute("""
                        INSERT OR REPLACE INTO kv_store (key, value)
                        VALUES (?, ?);
                    """, (key, value if value is not None else ""))
                elif command_type.upper() == "DELETE":
                    cursor.execute("DELETE FROM kv_store WHERE key = ?;", (key,))
            self.conn.commit()

    def get_kv(self, key: str) -> Optional[str]:
        """Retrieve key value from the state machine."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT value FROM kv_store WHERE key = ?;", (key,))
            row = cursor.fetchone()
            if row:
                return row["value"]
            return None

    def get_all_kv(self) -> Dict[str, str]:
        """Retrieve complete state machine key-value mapping."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT key, value FROM kv_store ORDER BY key ASC;")
            rows = cursor.fetchall()
            return {r["key"]: r["value"] for r in rows}
