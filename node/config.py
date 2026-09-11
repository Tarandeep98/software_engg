"""
Node Configuration Module
Reads configuration from environment variables, cluster_config.json, or defaults.
Supports running locally or across different devices under the same Wi-Fi.
"""

import json
import os
import socket
from typing import List, Optional


def get_lan_ip() -> str:
    """
    Attempts to detect the local IPv4 address on the Wi-Fi/LAN interface.
    Falls back to '127.0.0.1' if disconnected.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Does not actually transmit packets, just routes to pick the best interface
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


class Config:
    def __init__(self):
        # Unique identifier for this node (1, 2, 3, etc.)
        self.NODE_ID: int = int(os.getenv("NODE_ID", "1"))

        # Optional cluster configuration file path
        config_path = os.getenv("CLUSTER_CONFIG_FILE", "cluster_config.json")
        cluster_file_data = self._load_cluster_file(config_path)

        # Wi-Fi mode flag (tunes timeouts for network jitter across separate devices)
        wifi_mode_env = os.getenv("WIFI_MODE", "").lower() in ("1", "true", "yes")
        self.WIFI_MODE: bool = wifi_mode_env or cluster_file_data.get("wifi_mode", False)

        # Host interface: default to 0.0.0.0 so all Wi-Fi devices can reach this node
        self.HOST: str = os.getenv("HOST", "0.0.0.0")

        # Port assignment: environment variable > cluster_config.json > default
        node_cfg = cluster_file_data.get("nodes_by_id", {}).get(self.NODE_ID, {})
        default_port = node_cfg.get("port", 8000 + self.NODE_ID)
        self.PORT: int = int(os.getenv("PORT", str(default_port)))

        # Peers assignment: environment variable > cluster_config.json > default localhost
        raw_peers = os.getenv("PEERS", "")
        if raw_peers.strip():
            self.PEERS: List[str] = self._parse_peers(raw_peers)
        elif "peers" in node_cfg:
            self.PEERS = node_cfg["peers"]
        else:
            self.PEERS = self._parse_peers("")

        # SQLite database path for persistent state and key-value store
        data_dir = os.getenv("DATA_DIR", ".")
        os.makedirs(data_dir, exist_ok=True)
        self.DB_PATH: str = os.path.join(data_dir, f"node_{self.NODE_ID}.db")

        # Raft timing configuration
        # In Wi-Fi mode, relax timeouts slightly to prevent false elections on wireless jitter
        default_election_min = "300" if self.WIFI_MODE else "150"
        default_election_max = "600" if self.WIFI_MODE else "300"
        default_heartbeat = "100" if self.WIFI_MODE else "50"
        default_rpc_timeout = "1000" if self.WIFI_MODE else "300"

        self.ELECTION_TIMEOUT_MIN_MS: int = int(os.getenv("ELECTION_TIMEOUT_MIN_MS", default_election_min))
        self.ELECTION_TIMEOUT_MAX_MS: int = int(os.getenv("ELECTION_TIMEOUT_MAX_MS", default_election_max))
        self.HEARTBEAT_INTERVAL_MS: int = int(os.getenv("HEARTBEAT_INTERVAL_MS", default_heartbeat))
        self.RPC_TIMEOUT_MS: int = int(os.getenv("RPC_TIMEOUT_MS", default_rpc_timeout))

    def _load_cluster_file(self, path: str) -> dict:
        """Parses cluster_config.json if it exists."""
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            nodes = data.get("nodes", [])
            nodes_by_id = {}
            for n in nodes:
                nid = int(n["id"])
                # Compute peer URLs for each node in the config
                peers = []
                for other in nodes:
                    other_id = int(other["id"])
                    if other_id != nid:
                        peers.append(f"http://{other['host']}:{other['port']}")
                nodes_by_id[nid] = {
                    "host": n.get("host", "0.0.0.0"),
                    "port": int(n.get("port", 8000 + nid)),
                    "peers": peers
                }
            return {
                "wifi_mode": data.get("wifi_mode", True),
                "nodes_by_id": nodes_by_id
            }
        except Exception as e:
            print(f"[Config] Warning: Failed to load {path}: {e}")
            return {}

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

