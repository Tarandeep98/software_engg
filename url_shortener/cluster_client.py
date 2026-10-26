import os
import asyncio
from typing import List, Tuple, Optional
import httpx

# Helper to map node id (1‑based) -> URL from CLUSTER_NODES env or defaults
_DEFAULT_NODES = [
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",
    "http://127.0.0.1:8003",
]

class ClusterClient:
    """Simple async client that talks to the Raft KV cluster.
    It remembers the last known leader and retries according to the spec.
    """

    def __init__(self) -> None:
        nodes_env = os.getenv("CLUSTER_NODES")
        self.node_urls: List[str] = [u.rstrip("/") for u in (nodes_env.split(",") if nodes_env else _DEFAULT_NODES)]
        self.leader_index: int = 0  # 0‑based index into node_urls

    def _url_for_id(self, node_id: int) -> str:
        """Convert a 1‑based node id to a URL using the current list."""
        return self.node_urls[node_id - 1]

    async def _post_set(self, base_url: str, key: str, value: str, timeout: float = 1.0) -> Optional[httpx.Response]:
        try:
            async with httpx.AsyncClient() as client:
                return await client.post(f"{base_url}/kv/set", json={"key": key, "value": value}, timeout=timeout)
        except Exception:
            return None

    async def _get(self, base_url: str, key: str, timeout: float = 1.0) -> Optional[httpx.Response]:
        try:
            async with httpx.AsyncClient() as client:
                return await client.get(f"{base_url}/kv/get", params={"key": key}, timeout=timeout)
        except Exception:
            return None

    async def write(self, key: str, value: str) -> Tuple[dict, str, int]:
        """Write a KV pair, returning (response_json, node_url, retries_used).
        Retries follow the rules described in the task description.
        """
        retries = 0
        for round_num in range(5):
            start = self.leader_index
            for offset in range(len(self.node_urls)):
                idx = (start + offset) % len(self.node_urls)
                node = self.node_urls[idx]
                resp = await self._post_set(node, key, value)
                if resp is None:
                    retries += 1
                    continue
                if resp.status_code == 200:
                    self.leader_index = idx
                    return resp.json(), node, retries
                if resp.status_code == 421:
                    data = resp.json()
                    hint = data.get("leader_hint")
                    if hint:
                        hinted_node = self._url_for_id(int(hint))
                        retries += 1
                        hinted_resp = await self._post_set(hinted_node, key, value)
                        if hinted_resp and hinted_resp.status_code == 200:
                            self.leader_index = int(hint) - 1
                            return hinted_resp.json(), hinted_node, retries
                    else:
                        await asyncio.sleep(0.3)
                        retries += 1
                        continue
                if resp.status_code in (500, 503):
                    retries += 1
                    continue
            await asyncio.sleep(0.5)
        raise RuntimeError(f"Write failed after {retries} retries")

    async def read(self, key: str) -> Tuple[dict, str, int]:
        """Read a KV pair, returning (response_json, node_url, retries_used)."""
        retries = 0
        not_found_attempts = 0
        for round_num in range(5):
            start = self.leader_index
            for offset in range(len(self.node_urls)):
                idx = (start + offset) % len(self.node_urls)
                node = self.node_urls[idx]
                resp = await self._get(node, key)
                if resp is None:
                    retries += 1
                    continue
                if resp.status_code == 200:
                    self.leader_index = idx
                    return resp.json(), node, retries
                if resp.status_code == 404:
                    not_found_attempts += 1
                    if not_found_attempts < 3:
                        await asyncio.sleep(0.1)
                        retries += 1
                        continue
                    return {"error": "not found"}, node, retries
                retries += 1
                continue
            await asyncio.sleep(0.5)
        raise RuntimeError(f"Read failed after {retries} retries")

    async def status(self) -> Tuple[dict, str, int]:
        """Find the node whose role is Leader."""
        retries = 0
        for idx, node in enumerate(self.node_urls):
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.get(f"{node}/status", timeout=1.0)
                if resp.status_code == 200 and resp.json().get("role") == "Leader":
                    self.leader_index = idx
                    return resp.json(), node, retries
            except Exception:
                pass
            retries += 1
        raise RuntimeError(f"No leader found after {retries} retries")
