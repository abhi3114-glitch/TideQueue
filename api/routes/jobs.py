"""
TideQueue Jobs API Routes
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Any, Dict
from datetime import datetime

router = APIRouter(prefix="/jobs", tags=["Jobs"])


class JobCreate(BaseModel):
    """Request body for creating a job."""
    task: str
    args: List[Any] = []
    kwargs: Dict[str, Any] = {}
    queue: str = "default"
    priority: int = 0
    max_retries: int = 3


class JobResponse(BaseModel):
    """Response model for a job."""
    id: str
    name: str
    args: List[Any]
    kwargs: Dict[str, Any]
    queue: str
    priority: int
    status: str
    retry_count: int
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    worker_id: Optional[str] = None

    class Config:
        from_attributes = True


class JobListResponse(BaseModel):
    """Response for job listing."""
    jobs: List[JobResponse]
    total: int
    limit: int
    offset: int


def get_broker():
    """Get the shared broker instance."""
    from ..main import get_app_broker
    return get_app_broker()


@router.post("", response_model=dict)
async def create_job(job: JobCreate):
    """Submit a new job to the queue."""
    from tidequeue.models import Job, RetryPolicy
    
    broker = get_broker()
    
    new_job = Job.create(
        name=job.task,
        args=job.args,
        kwargs=job.kwargs,
        queue=job.queue,
        priority=job.priority,
        retry_policy=RetryPolicy(max_retries=job.max_retries),
    )
    
    job_id = broker.enqueue(new_job)
    
    return {
        "id": job_id,
        "status": "pending",
        "message": f"Job '{job.task}' submitted to queue '{job.queue}'"
    }


@router.get("", response_model=JobListResponse)
async def list_jobs(
    status: Optional[str] = None,
    queue: Optional[str] = None,
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
):
    """List jobs with optional filtering."""
    from tidequeue.models import JobStatus
    
    broker = get_broker()
    
    if status:
        try:
            job_status = JobStatus(status)
            jobs = broker.get_jobs_by_status(job_status, limit=limit)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
    else:
        jobs = broker.get_all_jobs(limit=limit, offset=offset)
    
    job_responses = [
        JobResponse(
            id=j.id,
            name=j.name,
            args=j.args,
            kwargs=j.kwargs,
            queue=j.queue,
            priority=j.priority,
            status=j.status.value,
            retry_count=j.retry_count,
            result=j.result,
            error=j.error,
            created_at=j.created_at.isoformat() if j.created_at else None,
            started_at=j.started_at.isoformat() if j.started_at else None,
            completed_at=j.completed_at.isoformat() if j.completed_at else None,
            worker_id=j.worker_id,
        )
        for j in jobs
    ]
    
    return JobListResponse(
        jobs=job_responses,
        total=len(job_responses),
        limit=limit,
        offset=offset,
    )


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    """Get a specific job by ID."""
    broker = get_broker()
    job = broker.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    
    return JobResponse(
        id=job.id,
        name=job.name,
        args=job.args,
        kwargs=job.kwargs,
        queue=job.queue,
        priority=job.priority,
        status=job.status.value,
        retry_count=job.retry_count,
        result=job.result,
        error=job.error,
        created_at=job.created_at.isoformat() if job.created_at else None,
        started_at=job.started_at.isoformat() if job.started_at else None,
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
        worker_id=job.worker_id,
    )


@router.delete("/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a pending job."""
    from tidequeue.exceptions import JobNotFoundError
    
    broker = get_broker()
    
    try:
        success = broker.cancel_job(job_id)
        if success:
            return {"message": f"Job {job_id} cancelled"}
        else:
            raise HTTPException(
                status_code=400,
                detail="Cannot cancel job (only pending/scheduled jobs can be cancelled)"
            )
    except JobNotFoundError:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")


@router.post("/{job_id}/retry")
async def retry_job(job_id: str):
    """Retry a failed/dead job."""
    from tidequeue.exceptions import JobNotFoundError
    
    broker = get_broker()
    
    try:
        success = broker.retry_job(job_id)
        if success:
            return {"message": f"Job {job_id} requeued for retry"}
        else:
            raise HTTPException(
                status_code=400,
                detail="Cannot retry job (only failed/dead/cancelled jobs can be retried)"
            )
    except JobNotFoundError:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
