"""
TideQueue Dead-Letter Queue API Routes
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any, Dict

router = APIRouter(prefix="/dlq", tags=["Dead-Letter Queue"])


class DLQJobResponse(BaseModel):
    """Response model for a DLQ job."""
    id: str
    task: str
    args: List[Any]
    kwargs: Dict[str, Any]
    queue: str
    retry_count: int
    error: Optional[str] = None
    reason: str
    moved_at: str


class DLQListResponse(BaseModel):
    """Response for DLQ listing."""
    jobs: List[DLQJobResponse]
    total: int


class DLQStats(BaseModel):
    """Statistics for the DLQ."""
    total: int
    by_task: Dict[str, int]
    by_error: Dict[str, int]


def get_dlq():
    """Get the shared DLQ instance."""
    from ..main import get_app_dlq
    return get_app_dlq()


def get_broker():
    """Get the shared broker instance."""
    from ..main import get_app_broker
    return get_app_broker()


@router.get("", response_model=DLQListResponse)
async def list_dlq_jobs(limit: int = 100):
    """List jobs in the dead-letter queue."""
    dlq = get_dlq()
    jobs = dlq.get_jobs(limit=limit)
    
    job_responses = [
        DLQJobResponse(
            id=j["id"],
            task=j["job"].name,
            args=j["job"].args,
            kwargs=j["job"].kwargs,
            queue=j["job"].queue,
            retry_count=j["job"].retry_count,
            error=j["job"].error,
            reason=j["reason"],
            moved_at=j["moved_at"],
        )
        for j in jobs
    ]
    
    return DLQListResponse(
        jobs=job_responses,
        total=len(job_responses),
    )


@router.get("/stats", response_model=DLQStats)
async def get_dlq_stats():
    """Get DLQ statistics."""
    dlq = get_dlq()
    stats = dlq.get_stats()
    
    return DLQStats(
        total=stats["total"],
        by_task=stats["by_task"],
        by_error=stats["by_error"],
    )


@router.get("/{job_id}", response_model=DLQJobResponse)
async def get_dlq_job(job_id: str):
    """Get a specific job from the DLQ."""
    dlq = get_dlq()
    job_entry = dlq.get_job(job_id)
    
    if not job_entry:
        raise HTTPException(status_code=404, detail=f"Job not found in DLQ: {job_id}")
    
    return DLQJobResponse(
        id=job_entry["id"],
        task=job_entry["job"].name,
        args=job_entry["job"].args,
        kwargs=job_entry["job"].kwargs,
        queue=job_entry["job"].queue,
        retry_count=job_entry["job"].retry_count,
        error=job_entry["job"].error,
        reason=job_entry["reason"],
        moved_at=job_entry["moved_at"],
    )


@router.post("/{job_id}/replay")
async def replay_dlq_job(job_id: str, reset_retry_count: bool = True):
    """Replay a job from the DLQ."""
    dlq = get_dlq()
    broker = get_broker()
    
    job = dlq.replay(job_id, reset_retry_count=reset_retry_count)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found in DLQ: {job_id}")
    
    # Re-enqueue the job
    broker.enqueue(job)
    
    return {
        "message": f"Job {job_id} replayed from DLQ",
        "new_status": "pending",
    }


@router.delete("/{job_id}")
async def purge_dlq_job(job_id: str):
    """Permanently remove a job from the DLQ."""
    dlq = get_dlq()
    
    success = dlq.purge(job_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Job not found in DLQ: {job_id}")
    
    return {"message": f"Job {job_id} purged from DLQ"}


@router.delete("")
async def purge_all_dlq():
    """Purge all jobs from the DLQ."""
    dlq = get_dlq()
    count = dlq.purge_all()
    
    return {
        "message": f"Purged {count} jobs from DLQ",
        "count": count,
    }
