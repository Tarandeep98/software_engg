"""
URL Shortener Integration Test
Requires:
  - Raft cluster running on ports 8001-8003
  - URL shortener running on port 9000

Run:
    python url_shortener/test_shortener.py
"""

import sys
import time
import httpx

SHORTENER = "http://127.0.0.1:9000"
NODES = [
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",
    "http://127.0.0.1:8003",
]


def find_leader():
    """Find the current Raft leader by polling /status on each node."""
    for node in NODES:
        try:
            r = httpx.get(f"{node}/status", timeout=1.0)
            if r.status_code == 200 and r.json().get("role") == "Leader":
                return node
        except Exception:
            pass
    raise RuntimeError("No leader found")


def main():
    killed_node = None
    try:
        # --- Step 1: Find leader ---
        leader = find_leader()
        print(f"PASS: Leader found at {leader}")

        # --- Step 2: Create a short link ---
        resp = httpx.post(
            f"{SHORTENER}/shorten",
            json={"url": "https://example.com/test-page"},
            timeout=5.0,
        )
        assert resp.status_code == 200, f"POST /shorten failed: {resp.status_code} {resp.text}"
        data = resp.json()
        code = data["code"]
        short_url = data["short_url"]
        print(f"PASS: Created short link {short_url} (code={code})")

        # --- Step 3: Follow the short link (expect 302) ---
        r = httpx.get(f"{SHORTENER}/{code}", follow_redirects=False, timeout=5.0)
        assert r.status_code == 302, f"Expected 302, got {r.status_code}"
        assert "example.com" in r.headers.get("location", "")
        print(f"PASS: GET /{code} returned 302 -> {r.headers['location']}")

        # --- Step 4: Kill the leader ---
        killed_node = leader
        kill_resp = httpx.post(f"{leader}/admin/kill", timeout=2.0)
        assert kill_resp.status_code == 200
        print(f"Killed leader at {leader}")

        # --- Step 5: Wait for failover and verify redirect still works ---
        success = False
        for _ in range(25):  # up to ~5 seconds
            time.sleep(0.2)
            try:
                r = httpx.get(f"{SHORTENER}/{code}", follow_redirects=False, timeout=2.0)
                if r.status_code == 302:
                    success = True
                    break
            except Exception:
                pass
        assert success, "Redirect did not succeed within 5 seconds after leader kill"
        print(f"PASS: GET /{code} returned 302 after failover")

        # --- Step 6: Create a new link with one node dead ---
        resp2 = httpx.post(
            f"{SHORTENER}/shorten",
            json={"url": "https://example.com/second-link"},
            timeout=5.0,
        )
        assert resp2.status_code == 200, f"POST /shorten with dead node failed: {resp2.status_code} {resp2.text}"
        code2 = resp2.json()["code"]
        print(f"PASS: Created second short link (code={code2}) with one node dead")

        print("\n=== ALL URL SHORTENER TESTS PASSED ===\n")

    except Exception as e:
        print(f"\nFAILED: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        # --- Revive killed node ---
        if killed_node:
            try:
                httpx.post(f"{killed_node}/admin/restart", timeout=2.0)
                print(f"Revived node at {killed_node}")
            except Exception:
                print(f"Warning: could not revive {killed_node}")


if __name__ == "__main__":
    main()
