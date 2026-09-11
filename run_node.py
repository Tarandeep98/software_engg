"""
Single Node Runner for Distributed Wi-Fi Raft Cluster
Run this on each separate device (laptop, computer) connected to the same Wi-Fi.

Usage examples:
    # Option 1: Using cluster_config.json
    python run_node.py --id 1

    # Option 2: Specifying peers directly via CLI
    python run_node.py --id 1 --port 8001 --peers http://172.16.79.154:8002,http://172.16.79.155:8003

    # Option 3: Force Wi-Fi timing profile
    python run_node.py --id 2 --wifi-mode
"""

import argparse
import os
import sys
import uvicorn

from node.config import get_lan_ip


def main():
    parser = argparse.ArgumentParser(
        description="Run an individual Raft Node on this device for multi-machine Wi-Fi cluster deployment."
    )
    parser.add_argument("--id", type=int, default=1, help="Unique integer ID for this node (e.g. 1, 2, 3)")
    parser.add_argument("--port", type=int, default=None, help="Port to bind (default: 8000 + ID)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host interface to bind (default: 0.0.0.0)")
    parser.add_argument("--peers", type=str, default=None, help="Comma-separated peer URLs (e.g. http://172.16.79.154:8002,http://172.16.79.155:8003)")
    parser.add_argument("--config", type=str, default="cluster_config.json", help="Path to cluster_config.json")
    parser.add_argument("--wifi-mode", action="store_true", default=None, help="Enable Wi-Fi timing adjustments (higher timeouts for wireless jitter)")
    parser.add_argument("--log-level", type=str, default="info", help="Uvicorn log level (info, warning, error)")

    args = parser.parse_args()

    port = args.port if args.port is not None else (8000 + args.id)
    lan_ip = get_lan_ip()

    # Pass configuration to environment before loading node modules
    os.environ["NODE_ID"] = str(args.id)
    os.environ["PORT"] = str(port)
    os.environ["HOST"] = args.host
    os.environ["CLUSTER_CONFIG_FILE"] = args.config

    if args.peers is not None:
        os.environ["PEERS"] = args.peers

    if args.wifi_mode is not None:
        os.environ["WIFI_MODE"] = "1" if args.wifi_mode else "0"
    else:
        # Automatically enable wifi mode if peers point to non-localhost IPs
        if args.peers and ("127.0.0.1" not in args.peers and "localhost" not in args.peers):
            os.environ["WIFI_MODE"] = "1"

    # Re-import config after environment is populated
    from node.config import config

    print("==================================================================")
    print(f"   Starting Distributed Raft Node {config.NODE_ID}")
    print("==================================================================")
    print(f" 📡 Local Wi-Fi IP    : {lan_ip}")
    print(f" 🔌 Listening Port    : {config.PORT}")
    print(f" 🌐 Bind Interface    : {config.HOST}")
    print(f" ⚡ Wi-Fi Mode        : {'Enabled (relaxed timeouts)' if config.WIFI_MODE else 'Disabled (fast local)'}")
    print(f" 👥 Configured Peers  :")
    if config.PEERS:
        for p in config.PEERS:
            print(f"    - {p}")
    else:
        print("    (None - standalone node)")
    print()
    print(" 🖥️ Access Dashboard & Status on THIS machine:")
    print(f"    👉 http://localhost:{config.PORT}/")
    print()
    if lan_ip != "127.0.0.1":
        print(" 📱 Access from OTHER DEVICES on the same Wi-Fi:")
        print(f"    👉 http://{lan_ip}:{config.PORT}/")
    print("==================================================================\n")

    uvicorn.run(
        "node.main:app",
        host=config.HOST,
        port=config.PORT,
        log_level=args.log_level.lower()
    )


if __name__ == "__main__":
    main()
