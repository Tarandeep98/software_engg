from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import string
import random
from cluster_client import ClusterClient

app = FastAPI(title="URL Shortener", version="1.0.0")

# CORS for browser interaction
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# client to talk to Raft KV cluster
client = ClusterClient()

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
INDEX_HTML = os.path.join(STATIC_DIR, "index.html")

def _generate_code() -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=6))

@app.get("/", include_in_schema=False)
async def serve_index():
    if os.path.exists(INDEX_HTML):
        return FileResponse(INDEX_HTML)
    return {"message": "index.html not found"}

@app.post("/shorten")
async def shorten(request: Request, payload: dict):
    url = payload.get("url")
    if not isinstance(url, str) or not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="URL must start with http:// or https://")
    try:
        # try up to 5 times to avoid collision (not safe under concurrency)
        for _ in range(5):
            code = _generate_code()
            key = f"url:{code}"
            # check existence
            read_resp, _, _ = await client.read(key)
            if read_resp.get("error") == "not found":
                # safe to use
                write_resp, node, retries = await client.write(key, url)
                host = request.headers.get("host")
                scheme = request.url.scheme
                short_url = f"{scheme}://{host}/{code}"
                return {
                    "code": code,
                    "short_url": short_url,
                    "node": node,
                    "retries": retries,
                    "status": write_resp.get("status", "ok"),
                }
        raise HTTPException(status_code=500, detail="Failed to generate unique short code")
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Cluster unavailable, try again")

@app.get("/{code}")
async def redirect_code(code: str):
    try:
        key = f"url:{code}"
        read_resp, _, _ = await client.read(key)
        if "value" in read_resp:
            return RedirectResponse(url=read_resp["value"], status_code=302)
        raise HTTPException(status_code=404, detail="Short code not found")
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Cluster unavailable, try again")
