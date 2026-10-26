"""
FastAPI Application Entrypoint for Raft Node
Exposes:
- Client-facing KV routes (/status, /log, /kv/get, /kv/set, /kv/delete, /kv/all)
- Inter-node Raft RPC routes (/raft/request_vote, /raft/append_entries)
- Admin failure simulation routes (/admin/kill, /admin/restart)
"""

import os
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .config import config
from .raft_state import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RaftNode,
    RequestVoteArgs,
    RequestVoteReply,
)
from .storage import Storage

FRONTEND_HTML = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html"))

# Initialize persistent SQLite storage with exact tables
storage = Storage(config.DB_PATH)

# Initialize Raft consensus state machine
raft_node = RaftNode(
    node_id=config.NODE_ID,
    peers=config.PEERS,
    storage=storage
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background consensus loop on startup, stop on shutdown."""
    await raft_node.start_background_loop()
    yield
    await raft_node.stop_background_loop()


app = FastAPI(
    title=f"Raft KV Node {config.NODE_ID}",
    description="Distributed Key-Value store node powered by complete Raft consensus.",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for browser access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------------------------------------------------------
# Middleware: Admin Kill / Dead State Guard
# -----------------------------------------------------------------------------

@app.middleware("http")
async def dead_node_filter(request: Request, call_next):
    """
    Simulates node crash when killed via /admin/kill.
    When is_dead is True:
    - /admin/restart is permitted so the node can be revived.
    - Dashboard UI (/, /dashboard) remains accessible so user can observe status and click Restart.
    - All other API routes return HTTP 503 Service Unavailable.
    """
    path = request.url.path
    if raft_node.is_dead and path not in ["/admin/restart", "/", "/dashboard"]:
        return Response(
            content='{"error": "node is dead (killed by admin)"}',
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            media_type="application/json"
        )
    return await call_next(request)


# -----------------------------------------------------------------------------
# Web Dashboard Serving
# -----------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
@app.get("/dashboard", include_in_schema=False)
async def serve_dashboard():
    """Serves the interactive Raft control dashboard directly via HTTP."""
    if os.path.exists(FRONTEND_HTML):
        return FileResponse(FRONTEND_HTML)
    return {"message": "Raft Node is active. frontend/index.html not found."}


# -----------------------------------------------------------------------------
# Pydantic Request Models for Client KV Routes
# -----------------------------------------------------------------------------

class SetKVRequest(BaseModel):
    key: str
    value: str


class DeleteKVRequest(BaseModel):
    key: str


# -----------------------------------------------------------------------------
# Client-Facing Routes
# -----------------------------------------------------------------------------

@app.get("/status")
async def get_status() -> Dict[str, Any]:
    """
    Returns current node status:
    {node_id, role, term, commit_index, last_log_index, leader_id, is_dead}
    """
    return raft_node.get_status()


@app.get("/log")
async def get_log() -> list:
    """
    Returns full replicated log as list of {index, term, command, committed}.
    """
    return raft_node.get_formatted_log()


@app.get("/kv/get")
async def kv_get(key: str) -> Dict[str, str]:
    """
    Fetch key from state machine. Returns {value} or 404.
    """
    val = storage.get_kv(key)
    if val is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Key '{key}' not found"
        )
    return {"key": key, "value": val}


@app.get("/kv/all")
async def kv_all() -> Dict[str, Any]:
    """
    Returns snapshot of all keys and values in the local state machine.
    """
    return {"store": storage.get_all_kv()}


@app.post("/kv/set")
async def kv_set(payload: SetKVRequest, response: Response):
    """
    Write key-value pair. If this node is not the leader,
    respond with 421 Misdirected Request containing {error: "not leader", leader_hint: ...}.
    Blocks until committed by majority, then returns success.
    """
    success, error, leader_hint = await raft_node.execute_client_command(
        command_type="SET",
        key=payload.key,
        value=payload.value
    )
    if not success:
        if error == "not leader":
            response.status_code = status.HTTP_421_MISDIRECTED_REQUEST
            return {"error": "not leader", "leader_hint": leader_hint}
        raise HTTPException(status_code=500, detail=error or "Failed to set value")
    return {"status": "ok", "key": payload.key, "value": payload.value}


@app.post("/kv/delete")
async def kv_delete(payload: DeleteKVRequest, response: Response):
    """
    Delete key. If this node is not the leader,
    respond with 421 Misdirected Request containing {error: "not leader", leader_hint: ...}.
    Blocks until committed by majority, then returns success.
    """
    success, error, leader_hint = await raft_node.execute_client_command(
        command_type="DELETE",
        key=payload.key,
        value=None
    )
    if not success:
        if error == "not leader":
            response.status_code = status.HTTP_421_MISDIRECTED_REQUEST
            return {"error": "not leader", "leader_hint": leader_hint}
        raise HTTPException(status_code=500, detail=error or "Failed to delete key")
    return {"status": "ok", "key": payload.key}


# -----------------------------------------------------------------------------
# Inter-Node Raft RPC Routes
# -----------------------------------------------------------------------------

@app.post("/raft/request_vote", response_model=RequestVoteReply)
async def raft_request_vote(args: RequestVoteArgs) -> RequestVoteReply:
    """
    Standard Raft RequestVote RPC (§5.2, §5.4).
    """
    return await raft_node.handle_request_vote(args)


@app.post("/raft/append_entries", response_model=AppendEntriesReply)
async def raft_append_entries(args: AppendEntriesArgs) -> AppendEntriesReply:
    """
    Standard Raft AppendEntries RPC (§5.2, §5.3).
    """
    return await raft_node.handle_append_entries(args)


# -----------------------------------------------------------------------------
# Admin / Test Routes
# -----------------------------------------------------------------------------

@app.post("/admin/kill")
async def admin_kill() -> Dict[str, Any]:
    """
    Sets internal 'dead' flag. Node ceases sending/responding to Raft RPCs
    and client requests (503), but process keeps running.
    """
    await raft_node.kill()
    return {"status": "killed", "node_id": config.NODE_ID}


@app.post("/admin/restart")
async def admin_restart() -> Dict[str, Any]:
    """
    Clears internal 'dead' flag. Node resumes as Follower, re-reads persisted
    state from storage, and begins catching up.
    """
    await raft_node.restart()
    return {"status": "restarted", "node_id": config.NODE_ID}
