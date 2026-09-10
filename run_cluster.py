"""
Cluster Runner Script
Starts all 3 Raft nodes (Node 1 on 8001, Node 2 on 8002, Node 3 on 8003) as subprocesses.
Handles clean shutdown on Ctrl+C.
"""

import os
import signal
import subprocess
import sys
import time

PYTHON_EXEC = sys.executable

NODES = [
    {
        "NODE_ID": "1",
        "PORT": "8001",
        "PEERS": "http://127.0.0.1:8002,http://127.0.0.1:8003"
    },
    {
        "NODE_ID": "2",
        "PORT": "8002",
        "PEERS": "http://127.0.0.1:8001,http://127.0.0.1:8003"
    },
    {
        "NODE_ID": "3",
        "PORT": "8003",
        "PEERS": "http://127.0.0.1:8001,http://127.0.0.1:8002"
    },
]


def main():
    print("================================================================")
    print(" Starting 3-Node Raft Cluster (Ports: 8001, 8002, 8003)...")
    print(" Press Ctrl+C at any time to stop all nodes cleanly.")
    print("================================================================")

    processes = []

    try:
        for node in NODES:
            env = os.environ.copy()
            env["NODE_ID"] = node["NODE_ID"]
            env["PORT"] = node["PORT"]
            env["PEERS"] = node["PEERS"]
            
            cmd = [
                PYTHON_EXEC, "-m", "uvicorn", "node.main:app",
                "--host", "127.0.0.1",
                "--port", node["PORT"],
                "--log-level", "warning"
            ]
            
            print(f"[Launcher] Starting Node {node['NODE_ID']} on http://127.0.0.1:{node['PORT']}...")
            p = subprocess.Popen(cmd, env=env)
            processes.append((node["NODE_ID"], p))
            time.sleep(0.3)

        print("\n[Launcher] All 3 nodes are running!")
        print("[Launcher] Open frontend/index.html in your browser to view the cluster.\n")

        # Keep running until interrupted
        while True:
            time.sleep(1)
            for nid, p in processes:
                if p.poll() is not None:
                    print(f"[Launcher] Warning: Node {nid} exited with code {p.returncode}")

    except KeyboardInterrupt:
        print("\n[Launcher] Shutting down all cluster nodes...")
    finally:
        for nid, p in processes:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    p.kill()
        print("[Launcher] All nodes stopped.")


if __name__ == "__main__":
    main()
