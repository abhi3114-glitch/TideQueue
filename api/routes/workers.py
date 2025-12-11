"""
TideQueue Workers API Routes
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional

router = APIRouter(prefix="/workers", tags=["Workers"])


class WorkerResponse(BaseModel):
    """Response model for a worker."""
    id: str
    status: str
    current_job_id: Optional[str] = None
    jobs_completed: int
    jobs_failed: int
    started_at: str
    last_heartbeat: str
    queues: List[str]


class WorkerPoolStats(BaseModel):
    """Statistics for the worker pool."""
    total_workers: int
    idle: int
    busy: int
    jobs_completed: int
    jobs_failed: int
    queues: List[str]


class ScaleRequest(BaseModel):
    """Request body for scaling workers."""
    size: int


def get_worker_pool():
    """Get the shared worker pool instance."""
    from ..main import get_app_pool
    return get_app_pool()


@router.get("", response_model=List[WorkerResponse])
async def list_workers():
    """List all workers and their status."""
    pool = get_worker_pool()
    
    if not pool:
        return []
    
    workers = pool.get_workers()
    
    return [
        WorkerResponse(
            id=w["id"],
            status=w["status"],
            current_job_id=w.get("current_job_id"),
            jobs_completed=w["jobs_completed"],
            jobs_failed=w["jobs_failed"],
            started_at=w["started_at"],
            last_heartbeat=w["last_heartbeat"],
            queues=w.get("queues", ["default"]),
        )
        for w in workers
    ]


@router.get("/stats", response_model=WorkerPoolStats)
async def get_worker_stats():
    """Get worker pool statistics."""
    pool = get_worker_pool()
    
    if not pool:
        return WorkerPoolStats(
            total_workers=0,
            idle=0,
            busy=0,
            jobs_completed=0,
            jobs_failed=0,
            queues=["default"],
        )
    
    stats = pool.get_stats()
    
    return WorkerPoolStats(
        total_workers=stats["total_workers"],
        idle=stats["idle"],
        busy=stats["busy"],
        jobs_completed=stats["jobs_completed"],
        jobs_failed=stats["jobs_failed"],
        queues=stats["queues"],
    )


@router.post("/scale")
async def scale_workers(request: ScaleRequest):
    """Scale the worker pool to a new size."""
    pool = get_worker_pool()
    
    if not pool:
        raise HTTPException(
            status_code=503,
            detail="Worker pool not initialized"
        )
    
    if request.size < 1:
        raise HTTPException(
            status_code=400,
            detail="Pool size must be at least 1"
        )
    
    if request.size > 100:
        raise HTTPException(
            status_code=400,
            detail="Pool size cannot exceed 100"
        )
    
    old_size = pool.worker_count
    pool.scale(request.size)
    
    return {
        "message": f"Worker pool scaled from {old_size} to {request.size}",
        "previous_size": old_size,
        "new_size": request.size,
    }
