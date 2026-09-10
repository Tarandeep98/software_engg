"""
Node Configuration Module
Reads configuration from environment variables or defaults for local multi-node execution.
"""

import os
from typing import List


class Config:
    def __init__(self):
        # Unique identifier for this node (1, 2, 3, etc.)
        self.NODE_ID: int = int(os.getenv("NODE_ID", "1"))
        
        # HTTP port this node binds to (8001, 8002, 8003, etc.)
        self.PORT: int = int(os.getenv("PORT", str(8000 + self.NODE_ID)))
        
        # Host interface
        self.HOST: str = os.getenv("HOST", "127.0.0.1")
        
        # Comma-separated list of peer addresses (e.g. "localhost:8002,localhost:8003" or "http://127.0.0.1:8002")
        raw_peers = os.getenv("PEERS", "")
        self.PEERS: List[str] = self._parse_peers(raw_peers)
        
        # SQLite database path for persistent state and key-value store
        data_dir = os.getenv("DATA_DIR", ".")
        os.makedirs(data_dir, exist_ok=True)
        self.DB_PATH: str = os.path.join(data_dir, f"node_{self.NODE_ID}.db")
        
        # Raft timing configuration (as specified in Raft paper and prompt)
        # Election timeout is randomized between 150ms and 300ms
        self.ELECTION_TIMEOUT_MIN_MS: int = int(os.getenv("ELECTION_TIMEOUT_MIN_MS", "150"))
        self.ELECTION_TIMEOUT_MAX_MS: int = int(os.getenv("ELECTION_TIMEOUT_MAX_MS", "300"))
        # Heartbeat interval is 50ms
        self.HEARTBEAT_INTERVAL_MS: int = int(os.getenv("HEARTBEAT_INTERVAL_MS", "50"))

    def _parse_peers(self, raw_peers: str) -> List[str]:
        """Normalize peer addresses to fully qualified HTTP URLs."""
        if not raw_peers.strip():
            # Default 3-node cluster configuration if not provided
            default_peers = {
                1: ["http://127.0.0.1:8002", "http://127.0.0.1:8003"],
                2: ["http://127.0.0.1:8001", "http://127.0.0.1:8003"],
                3: ["http://127.0.0.1:8001", "http://127.0.0.1:8002"],
            }
            return default_peers.get(self.NODE_ID, [])
        
        peers = []
        for p in raw_peers.split(","):
            p = p.strip()
            if not p:
                continue
            if not p.startswith("http://") and not p.startswith("https://"):
                p = f"http://{p}"
            peers.append(p)
        return peers


config = Config()
