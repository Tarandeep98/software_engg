"""
Integration and Verification Test Suite for 3-Node Raft Cluster
Tests:
1. Leader Election & Quorum
2. Client KV SET on Leader & Log Replication across Quorum
3. Follower 421 Misdirected Request with leader_hint
4. State Machine consistency (/kv/get on all nodes)
5. Admin Kill (/admin/kill) simulating node crash & 503 responses
6. Failover: Remaining 2 nodes elect a new Leader & continue committing writes
7. Admin Restart (/admin/restart) & log catch-up on revived node
8. Client KV DELETE operation
"""

import sys
import time
import httpx

BASE_URLS = {
    1: "http://127.0.0.1:8001",
    2: "http://127.0.0.1:8002",
    3: "http://127.0.0.1:8003"
}

def wait_for_leader(timeout=5.0):
    start = time.time()
    while time.time() - start < timeout:
        leaders = []
        for nid, url in BASE_URLS.items():
            try:
                r = httpx.get(f"{url}/status", timeout=1.0)
                if r.status_code == 200 and r.json().get("state") == "Leader":
                    leaders.append((nid, r.json()))
            except Exception:
                pass
        if len(leaders) == 1:
            return leaders[0][0], leaders[0][1]
        time.sleep(0.2)
    raise TimeoutError("No single leader established within timeout")

def run_tests():
    print("\n--- TEST 1: Cluster Health & Leader Election ---")
    leader_id, status = wait_for_leader()
    print(f"PASS: Node {leader_id} is elected Leader at Term {status['term']}.")

    # Verify all 3 nodes agree on term
    for nid, url in BASE_URLS.items():
        r = httpx.get(f"{url}/status", timeout=1.0)
        assert r.status_code == 200
        data = r.json()
        print(f"  Node {nid}: State={data['state']}, Term={data['term']}, Commit={data['commit_index']}")
        assert data["term"] == status["term"]
    print("PASS: All nodes agree on current term.")

    print("\n--- TEST 2: Client KV SET on Leader & Replication ---")
    leader_url = BASE_URLS[leader_id]
    set_resp = httpx.post(f"{leader_url}/kv/set", json={"key": "course", "value": "Distributed Systems"}, timeout=3.0)
    assert set_resp.status_code == 200, f"Expected 200, got {set_resp.status_code}: {set_resp.text}"
    print(f"PASS: SET 'course'='Distributed Systems' on Leader Node {leader_id}: {set_resp.json()}")

    # Give 100ms for follower heartbeats to advance commit index
    time.sleep(0.2)

    for nid, url in BASE_URLS.items():
        r = httpx.get(f"{url}/kv/get?key=course", timeout=1.0)
        assert r.status_code == 200, f"Node {nid} failed GET: {r.status_code} {r.text}"
        assert r.json()["value"] == "Distributed Systems"
        
        # Verify log
        log_r = httpx.get(f"{url}/log", timeout=1.0)
        assert log_r.status_code == 200
        entries = log_r.json()
        assert len(entries) >= 1
        assert entries[0]["committed"] is True
        print(f"  Node {nid}: Log has {len(entries)} entries, entry #1 committed=True, KV read value='{r.json()['value']}'")
    print("PASS: Log entry replicated and committed across all nodes.")

    print("\n--- TEST 3: 421 Misdirected Request with leader_hint ---")
    follower_id = 2 if leader_id != 2 else 3
    follower_url = BASE_URLS[follower_id]
    f_resp = httpx.post(f"{follower_url}/kv/set", json={"key": "test421", "value": "val"}, timeout=2.0)
    assert f_resp.status_code == 421, f"Expected 421, got {f_resp.status_code}: {f_resp.text}"
    body = f_resp.json()
    assert body.get("error") == "not leader"
    assert body.get("leader_hint") == leader_id, f"Expected leader_hint={leader_id}, got {body.get('leader_hint')}"
    print(f"PASS: Node {follower_id} returned HTTP 421 with leader_hint={body['leader_hint']}.")

    print("\n--- TEST 4: Admin Kill on Leader & Failover Election ---")
    kill_resp = httpx.post(f"{leader_url}/admin/kill", timeout=2.0)
    assert kill_resp.status_code == 200
    print(f"Killed Leader Node {leader_id}.")

    # Verify killed node returns 503
    dead_resp = httpx.get(f"{leader_url}/status", timeout=1.0)
    assert dead_resp.status_code == 503, f"Expected 503 for killed node, got {dead_resp.status_code}"
    print(f"PASS: Killed Node {leader_id} correctly returns HTTP 503 on requests.")

    # Wait for remaining 2 nodes to elect a new leader
    print("Waiting for failover election among remaining 2 nodes...")
    time.sleep(0.6)
    new_leader_id = None
    for nid in [n for n in [1, 2, 3] if n != leader_id]:
        r = httpx.get(f"{BASE_URLS[nid]}/status", timeout=1.0)
        if r.status_code == 200 and r.json().get("state") == "Leader":
            new_leader_id = nid
            break
    assert new_leader_id is not None, "A new leader was not elected among survivors!"
    print(f"PASS: Failover succeeded! Node {new_leader_id} is the new Leader.")

    print("\n--- TEST 5: Write with 1 Node Down (Quorum 2/3) ---")
    new_leader_url = BASE_URLS[new_leader_id]
    w_resp = httpx.post(f"{new_leader_url}/kv/set", json={"key": "during_partition", "value": "quorum_works"}, timeout=3.0)
    assert w_resp.status_code == 200
    print("PASS: Successfully committed new write while 1 node is dead.")

    print("\n--- TEST 6: Admin Restart & Catch-up ---")
    restart_resp = httpx.post(f"{leader_url}/admin/restart", timeout=2.0)
    assert restart_resp.status_code == 200
    print(f"Restarted Node {leader_id}.")

    # Wait for leader heartbeats to catch up the restarted node
    time.sleep(0.5)

    revived_status = httpx.get(f"{leader_url}/status", timeout=1.0).json()
    assert revived_status["is_dead"] is False
    assert revived_status["state"] == "Follower"
    print(f"PASS: Revived Node {leader_id} rejoined as Follower.")

    # Check that restarted node caught up on the entry written while it was dead
    revived_kv = httpx.get(f"{leader_url}/kv/get?key=during_partition", timeout=1.0)
    assert revived_kv.status_code == 200, f"Revived node missing caught-up key: {revived_kv.text}"
    assert revived_kv.json()["value"] == "quorum_works"
    print(f"PASS: Revived Node {leader_id} caught up on missed writes! State machine is synchronized.")

    print("\n--- TEST 7: Client KV DELETE ---")
    del_resp = httpx.post(f"{new_leader_url}/kv/delete", json={"key": "test421"}, timeout=3.0)
    assert del_resp.status_code == 200
    time.sleep(0.2)
    # Verify 404 for deleted key
    get_deleted = httpx.get(f"{new_leader_url}/kv/get?key=test421", timeout=1.0)
    assert get_deleted.status_code == 404
    print("PASS: DELETE operation committed and key returned 404.")

    print("\n=================================================")
    print(" ALL 7 RAFT & DISTRIBUTED KV TESTS PASSED! [SUCCESS]")
    print("=================================================\n")

if __name__ == "__main__":
    run_tests()
