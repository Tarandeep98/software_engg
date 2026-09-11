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
Open `frontend/index.html` in any modern web browser, or directly navigate to:
- `http://localhost:8001/`

---

## 📶 Running Across Different Devices on the Same Wi-Fi

You can use the cluster across Wi-Fi in two ways:

### Mode A: Access Cluster & Dashboard from Phones / Tablets / Other Laptops
1. Start the cluster normally on your main PC:
   ```bash
   python run_cluster.py
   ```
2. The launcher will automatically detect and print your local Wi-Fi IP (e.g., `172.16.79.153`).
3. On any phone, tablet, or laptop connected to the **same Wi-Fi network**, open your browser and navigate to:
   ```
   http://<YOUR_PC_WIFI_IP>:8001/
   ```
   *(e.g., `http://172.16.79.153:8001/`)*

### Mode B: True Multi-Device Cluster (Nodes on Separate Computers)
To run Node 1 on Laptop A, Node 2 on Laptop B, and Node 3 on Laptop C:

1. **Find each laptop's Wi-Fi IP** by running `ipconfig` in Command Prompt.
2. **Configure `cluster_config.json`**:
   Copy `cluster_config.json.example` to `cluster_config.json` on each laptop and set the IP addresses:
   ```json
   {
     "wifi_mode": true,
     "nodes": [
       {"id": 1, "host": "172.16.79.153", "port": 8001},
       {"id": 2, "host": "172.16.79.154", "port": 8002},
       {"id": 3, "host": "172.16.79.155", "port": 8003}
     ]
   }
   ```
3. **Launch the respective node on each device**:
   - On Laptop A: `python run_node.py --id 1`
   - On Laptop B: `python run_node.py --id 2`
   - On Laptop C: `python run_node.py --id 3`
   *(Or without a config file: `python run_node.py --id 1 --port 8001 --peers http://<IP_B>:8002,http://<IP_C>:8003`)*

### 🛡️ Windows Firewall Notice
If other devices cannot connect, Windows Firewall might be blocking inbound ports 8001-8003.
Run `setup_firewall.bat` as Administrator (or right-click `setup_firewall.ps1` -> Run with PowerShell) to open the ports.

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
