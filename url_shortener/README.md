# URL Shortener

Run the Raft KV cluster (nodes on ports 8001‑8003) **first**.
Then start the shortener service on port 9000:

```bash
uvicorn url_shortener.app:app --host 0.0.0.0 --port 9000
```

Run the test script:

```bash
python url_shortener/test_shortener.py
```

The service uses the same cluster endpoints via `CLUSTER_NODES` env var.
