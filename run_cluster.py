"""
Cluster Runner Script
Starts all 3 Raft nodes (Node 1 on 8001, Node 2 on 8002, Node 3 on 8003) as subprocesses.
Binds to 0.0.0.0 so that any phone, tablet, or laptop on the same Wi-Fi can access the cluster.
Handles clean shutdown on Ctrl+C.
"""

import argparse
import os
import subprocess
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from node.config import get_lan_ip

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
    parser = argparse.ArgumentParser(description="Run 3-Node Raft Cluster with local and Wi-Fi access.")
    parser.add_argument("--host", default="0.0.0.0", help="Host address to bind (default: 0.0.0.0 for LAN/Wi-Fi)")
    args = parser.parse_args()

    lan_ip = get_lan_ip()

    print("==================================================================")
    print("   Starting 3-Node Raft Cluster (Ports: 8001, 8002, 8003)")
    print(f"   Binding Host   : {args.host}")
    print(f"   Detected Wi-Fi : {lan_ip}")
    print("   Press Ctrl+C at any time to stop all nodes cleanly.")
    print("==================================================================")

    processes = []

    try:
        for node in NODES:
            env = os.environ.copy()
            env["NODE_ID"] = node["NODE_ID"]
            env["PORT"] = node["PORT"]
            env["PEERS"] = node["PEERS"]
            env["HOST"] = args.host
            
            cmd = [
                PYTHON_EXEC, "-m", "uvicorn", "node.main:app",
                "--host", args.host,
                "--port", node["PORT"],
                "--log-level", "warning"
            ]
            
            print(f"[Launcher] Starting Node {node['NODE_ID']} on port {node['PORT']}...")
            p = subprocess.Popen(cmd, env=env)
            processes.append((node["NODE_ID"], p))
            time.sleep(0.3)

        print("\n==================================================================")
        print(" [Launcher] All 3 nodes are running successfully!")
        print("==================================================================")
        print(" 💻 Access on THIS machine:")
        print("    👉 Node 1: http://localhost:8001/")
        print("    👉 Node 2: http://localhost:8002/")
        print("    👉 Node 3: http://localhost:8003/")
        print()
        if lan_ip != "127.0.0.1":
            print(" 📱 Access from ANY DEVICE on the same Wi-Fi (Phone, Tablet, Laptop):")
            print(f"    👉 Node 1: http://{lan_ip}:8001/")
            print(f"    👉 Node 2: http://{lan_ip}:8002/")
            print(f"    👉 Node 3: http://{lan_ip}:8003/")
            print()
            print(" (Note: If other devices cannot connect, check Windows Firewall: run setup_firewall.bat)")
        print("==================================================================\n")

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
