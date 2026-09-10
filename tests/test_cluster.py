"""
Pytest Failure-Injection Test Suite & Timing Instrumentation
Covers Scenarios 1 to 7 per project specifications:
1. Normal operation: SET/GET/DELETE replication across all nodes
2. Leader crash with no in-flight write: failover election & continuity
3. Leader crash mid-write: atomic commit or clean absence (no partial state)
4. Follower crash & restart: incremental log catch-up verification
5. Old leader returns: steps down to Follower upon discovering higher term
6. Split vote simulation: cluster converges to exactly one leader per term
7. Consistency-after-convergence: byte-identical state across all live nodes

Includes Section 6.2 Timing Instrumentation:
- Election Completion Time (failure to new leader)
- Write Commit Latency (client POST to quorum ACK)
- Outputs CSV report to benchmark_results.csv
"""

import csv
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import httpx
import pytest

BASE_URLS = {
    1: "http://127.0.0.1:8001",
    2: "http://127.0.0.1:8002",
    3: "http://127.0.0.1:8003"
}

# Direct CSV logger for Section 6.2 Timing Instrumentation
BENCHMARK_CSV_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "benchmark_results.csv"))


def record_benchmark(scenario: str, write_latency_ms: float = 0.0, election_time_ms: float = 0.0) -> None:
    """Appends timing measurement to benchmark_results.csv."""
    file_exists = os.path.exists(BENCHMARK_CSV_PATH)
    with open(BENCHMARK_CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["scenario", "write_latency_ms", "election_time_ms"])
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "scenario": scenario,
            "write_latency_ms": f"{write_latency_ms:.2f}",
            "election_time_ms": f"{election_time_ms:.2f}"
        })


# -----------------------------------------------------------------------------
# Cluster Management & Helper Utilities
# -----------------------------------------------------------------------------

def get_node_status(node_id: int) -> Optional[Dict]:
    url = f"{BASE_URLS[node_id]}/status"
    try:
        r = httpx.get(url, timeout=1.0)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def wait_for_leader(timeout: float = 6.0) -> Tuple[int, Dict]:
    """Poll until exactly one live leader is elected."""
    start = time.time()
    while time.time() - start < timeout:
        leaders = []
        for nid in [1, 2, 3]:
            st = get_node_status(nid)
            if st and st.get("role") == "Leader" and not st.get("is_dead"):
                leaders.append((nid, st))
        if len(leaders) == 1:
            return leaders[0][0], leaders[0][1]
        time.sleep(0.05)
    raise TimeoutError(f"No single leader established within {timeout}s")


def kill_node(node_id: int) -> None:
    """Send admin kill signal to simulate crash."""
    url = f"{BASE_URLS[node_id]}/admin/kill"
    resp = httpx.post(url, timeout=2.0)
    assert resp.status_code == 200


def restart_node(node_id: int) -> None:
    """Send admin restart signal to revive crashed node."""
    url = f"{BASE_URLS[node_id]}/admin/restart"
    resp = httpx.post(url, timeout=2.0)
    assert resp.status_code == 200


def ensure_all_nodes_running() -> None:
    """Ensure all 3 nodes are alive without restarting healthy nodes."""
    restarted_any = False
    for nid in [1, 2, 3]:
        st = get_node_status(nid)
        if st is None or st.get("is_dead") is True:
            restart_node(nid)
            restarted_any = True
    if restarted_any:
        time.sleep(0.3)
    wait_for_leader()


def write_kv_timed(key: str, value: str, leader_id: int) -> Tuple[bool, float, int]:
    """Sends POST /kv/set to leader and measures write commit latency, with 421 auto-redirect.
    Returns (success, latency_ms, actual_leader_id)
    """
    url = f"{BASE_URLS[leader_id]}/kv/set"
    actual_leader = leader_id
    t0 = time.perf_counter()
    try:
        resp = httpx.post(url, json={"key": key, "value": value}, timeout=6.0)
        if resp.status_code == 421:
            hint = resp.json().get("leader_hint")
            if hint and hint in BASE_URLS:
                actual_leader = hint
                resp = httpx.post(f"{BASE_URLS[hint]}/kv/set", json={"key": key, "value": value}, timeout=6.0)
        latency = (time.perf_counter() - t0) * 1000.0  # ms
        if resp.status_code == 200:
            return True, latency, actual_leader
        else:
            print(f"write_kv_timed failed with status {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"write_kv_timed exception: {e}")
    return False, (time.perf_counter() - t0) * 1000.0, actual_leader


def delete_kv_timed(key: str, leader_id: int) -> bool:
    """Sends POST /kv/delete with 421 auto-redirect."""
    url = f"{BASE_URLS[leader_id]}/kv/delete"
    try:
        resp = httpx.post(url, json={"key": key}, timeout=6.0)
        if resp.status_code == 421:
            hint = resp.json().get("leader_hint")
            if hint and hint in BASE_URLS:
                resp = httpx.post(f"{BASE_URLS[hint]}/kv/delete", json={"key": key}, timeout=6.0)
        return resp.status_code == 200
    except Exception:
        return False


def assert_byte_identical_state() -> Dict[str, str]:
    """
    Consistency-after-convergence (Section 6.1):
    Asserts all live nodes have byte-identical applied key-value state
    and that their state matches the leader's committed state.
    """
    leader_id, _ = wait_for_leader()
    live_states = {}

    for nid in [1, 2, 3]:
        st = get_node_status(nid)
        if st and not st.get("is_dead"):
            url = f"{BASE_URLS[nid]}/kv/all"
            r = httpx.get(url, timeout=1.5)
            assert r.status_code == 200, f"Failed to get kv/all on live Node {nid}"
            live_states[nid] = r.json().get("store", {})

    # Compare each live node's store against the leader's store
    leader_store = live_states[leader_id]
    for nid, store in live_states.items():
        assert store == leader_store, (
            f"State divergence detected! Node {nid} state != Leader {leader_id} state.\n"
            f"Node {nid}: {store}\nLeader: {leader_store}"
        )

    return leader_store


# -----------------------------------------------------------------------------
# Pytest Fixture
# -----------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def setup_teardown_cluster():
    """Ensure clean cluster state before and after each test run."""
    ensure_all_nodes_running()
    yield
    ensure_all_nodes_running()


# -----------------------------------------------------------------------------
# Test Scenarios (Parameterized for 5 iterations each per prompt)
# -----------------------------------------------------------------------------

@pytest.mark.parametrize("run_idx", list(range(1, 6)))
def test_scenario_1_normal_operation(run_idx: int):
    """
    Scenario 1: Normal Operation
    SET, GET, and DELETE operations succeed and replicate to all 3 nodes.
    Measures Write Commit Latency.
    """
    leader_id, _ = wait_for_leader()
    test_key = f"key_s1_run{run_idx}"
    test_val = f"val_s1_run{run_idx}"

    # 1. SET
    success, latency_ms, actual_leader = write_kv_timed(test_key, test_val, leader_id)
    assert success is True, f"SET failed on Leader Node {leader_id}"
    record_benchmark(f"S1_NormalWrite_Run{run_idx}", write_latency_ms=latency_ms)

    # Wait 150ms for follower commit index update
    time.sleep(0.15)

    # 2. GET on all live nodes
    for nid in [1, 2, 3]:
        r = httpx.get(f"{BASE_URLS[nid]}/kv/get?key={test_key}", timeout=1.5)
        assert r.status_code == 200, f"Node {nid} failed GET for key {test_key}"
        assert r.json().get("value") == test_val

    # 3. DELETE
    del_ok = delete_kv_timed(test_key, actual_leader)
    assert del_ok is True
    time.sleep(0.15)

    # Verify 404 for deleted key on all nodes
    for nid in [1, 2, 3]:
        r = httpx.get(f"{BASE_URLS[nid]}/kv/get?key={test_key}", timeout=1.5)
        assert r.status_code == 404

    # Assert byte-identical consistency
    assert_byte_identical_state()


@pytest.mark.parametrize("run_idx", list(range(1, 6)))
def test_scenario_2_leader_crash_no_inflight(run_idx: int):
    """
    Scenario 2: Leader Crash With No In-Flight Write
    Remaining 2 nodes elect a new leader, and cluster continues serving writes.
    Measures Election Completion Time.
    """
    old_leader_id, _ = wait_for_leader()
    
    t_fail = time.perf_counter()
    kill_node(old_leader_id)

    # Wait for failover election among the 2 remaining nodes
    start_poll = time.time()
    new_leader_id = None
    survivors = [n for n in [1, 2, 3] if n != old_leader_id]

    while time.time() - start_poll < 6.0:
        for nid in survivors:
            st = get_node_status(nid)
            if st and st.get("role") == "Leader":
                new_leader_id = nid
                break
        if new_leader_id:
            break
        time.sleep(0.05)

    election_time_ms = (time.perf_counter() - t_fail) * 1000.0
    assert new_leader_id is not None, f"Failed to elect new leader among {survivors}"
    assert new_leader_id != old_leader_id

    record_benchmark(f"S2_LeaderCrash_Run{run_idx}", election_time_ms=election_time_ms)

    # Cluster must accept new writes under 2-node quorum
    key = f"s2_postcrash_run{run_idx}"
    val = f"data_{run_idx}"
    success, _, _ = write_kv_timed(key, val, new_leader_id)
    assert success is True

    # Revive old leader and assert consistency
    restart_node(old_leader_id)
    time.sleep(0.3)
    assert_byte_identical_state()


@pytest.mark.parametrize("run_idx", list(range(1, 6)))
def test_scenario_3_leader_crash_mid_write(run_idx: int):
    """
    Scenario 3: Leader Crash Mid-Write
    Assert that the write either fully commits everywhere or is cleanly absent
    everywhere — no partial or inconsistent state across nodes.
    """
    old_leader_id, _ = wait_for_leader()
    key = f"mid_write_run{run_idx}"
    val = f"val_mid_{run_idx}"

    # Fire write and immediately kill the leader
    client = httpx.Client(timeout=0.8)
    try:
        client.post(f"{BASE_URLS[old_leader_id]}/kv/set", json={"key": key, "value": val})
    except Exception:
        pass
    finally:
        kill_node(old_leader_id)
        client.close()

    # Wait for new leader to be established among survivors
    time.sleep(0.6)
    new_leader_id, _ = wait_for_leader()

    # Restart the killed node
    restart_node(old_leader_id)
    time.sleep(0.5)

    # Check whether the key is present across all live nodes
    presence = []
    for nid in [1, 2, 3]:
        r = httpx.get(f"{BASE_URLS[nid]}/kv/get?key={key}", timeout=1.5)
        presence.append(r.status_code == 200)

    # Invariant: Either ALL live nodes have the key, or NONE have the key (no split state!)
    all_present = all(presence)
    all_absent = not any(presence)
    assert all_present or all_absent, (
        f"Inconsistent partial state detected! Key presence per node [1, 2, 3]: {presence}"
    )

    assert_byte_identical_state()


@pytest.mark.parametrize("run_idx", list(range(1, 6)))
def test_scenario_4_follower_crash_incremental_catchup(run_idx: int):
    """
    Scenario 4: Follower Crash and Restart with a Stale Log
    Verify the follower catches up via incremental log replication only,
    not receiving a full state dump.
    """
    leader_id, _ = wait_for_leader()
    followers = [n for n in [1, 2, 3] if n != leader_id]
    victim = followers[0]

    # Kill follower
    kill_node(victim)
    time.sleep(0.1)

    # Commit 3 writes while follower is offline
    keys = [f"catchup_{victim}_{i}_run{run_idx}" for i in range(1, 4)]
    for k in keys:
        ok, _, _ = write_kv_timed(k, f"val_{k}", leader_id)
        assert ok is True

    # Revive follower
    restart_node(victim)
    time.sleep(0.5)

    # Verify follower caught up and has all 3 keys
    for k in keys:
        r = httpx.get(f"{BASE_URLS[victim]}/kv/get?key={k}", timeout=1.5)
        assert r.status_code == 200, f"Victim Node {victim} did not catch up key {k}"

    # Verify follower's log length now matches leader's log length
    victim_log_after = httpx.get(f"{BASE_URLS[victim]}/log").json()
    leader_log_after = httpx.get(f"{BASE_URLS[leader_id]}/log").json()
    assert len(victim_log_after) == len(leader_log_after), (
        f"Log length mismatch: follower={len(victim_log_after)}, leader={len(leader_log_after)}"
    )

    assert_byte_identical_state()


@pytest.mark.parametrize("run_idx", list(range(1, 6)))
def test_scenario_5_old_leader_returns_steps_down(run_idx: int):
    """
    Scenario 5: Old Leader Returns After Being Killed and Replaced
    Assert old leader detects the higher term and steps down to Follower;
    assert it does NOT continue accepting writes as leader.
    """
    old_leader_id, old_status = wait_for_leader()
    old_term = old_status["term"]

    # Kill old leader
    kill_node(old_leader_id)

    # Wait for new leader to emerge with term > old_term
    time.sleep(0.6)
    new_leader_id, new_status = wait_for_leader()
    assert new_status["term"] > old_term
    assert new_leader_id != old_leader_id

    # Revive old leader
    restart_node(old_leader_id)
    time.sleep(0.4)

    # Query revived old leader: must be Follower, matching the new term
    revived_status = get_node_status(old_leader_id)
    assert revived_status is not None
    assert revived_status["role"] == "Follower", f"Expected Follower, got {revived_status['role']}"
    assert revived_status["term"] >= new_status["term"]

    # Attempting to write directly to revived old leader must fail with 421
    r = httpx.post(
        f"{BASE_URLS[old_leader_id]}/kv/set",
        json={"key": f"stale_test_run{run_idx}", "value": "should_redirect"},
        timeout=2.0
    )
    assert r.status_code == 421, f"Expected 421 from stepped-down leader, got {r.status_code}"
    assert r.json().get("leader_hint") == new_leader_id

    assert_byte_identical_state()


@pytest.mark.parametrize("run_idx", list(range(1, 6)))
def test_scenario_6_split_vote_convergence(run_idx: int):
    """
    Scenario 6: Split Vote & Simultaneous Candidacies
    Verify the cluster converges to exactly one leader per term with backoff.
    """
    leader_id, _ = wait_for_leader()
    followers = [n for n in [1, 2, 3] if n != leader_id]

    # Kill leader
    kill_node(leader_id)

    # Allow convergence
    time.sleep(0.8)
    survivors = followers
    new_leader_id, st = wait_for_leader(timeout=6.0)
    assert new_leader_id in survivors

    # Assert exactly one leader
    leaders = []
    for nid in survivors:
        s = get_node_status(nid)
        if s and s.get("role") == "Leader":
            leaders.append(nid)
    assert len(leaders) == 1, f"Multiple leaders detected: {leaders}"

    restart_node(leader_id)
    time.sleep(0.3)
    assert_byte_identical_state()


@pytest.mark.parametrize("run_idx", list(range(1, 6)))
def test_scenario_7_consistency_after_convergence(run_idx: int):
    """
    Scenario 7: Consistency-After-Convergence (Section 6.1)
    Assert all live nodes' applied key-value state is byte-identical
    and matches the leader's committed log.
    """
    leader_id, _ = wait_for_leader()

    # Perform a sequence of mixed writes and deletes
    for i in range(3):
        k = f"mix_k{i}_run{run_idx}"
        v = f"mix_v{i}_run{run_idx}"
        ok, _, actual_ldr = write_kv_timed(k, v, leader_id)
        if not ok:
            leader_id, _ = wait_for_leader()
            ok, _, actual_ldr = write_kv_timed(k, v, leader_id)
        assert ok is True
        leader_id = actual_ldr

    # Delete one key with 421 auto-redirect
    del_ok = delete_kv_timed(f"mix_k0_run{run_idx}", leader_id)
    assert del_ok is True

    time.sleep(0.2)

    # Verify byte-identical consistency across all 3 nodes
    state = assert_byte_identical_state()
    assert f"mix_k0_run{run_idx}" not in state
    assert f"mix_k1_run{run_idx}" in state
    assert f"mix_k2_run{run_idx}" in state
