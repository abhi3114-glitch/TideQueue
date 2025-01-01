"""
TideQueue Queues API Routes
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Dict

router = APIRouter(prefix="/queues", tags=["Queues"])


class QueueStats(BaseModel):
    """Statistics for a queue."""
    name: str
    pending: int
    running: int
    completed: int
    failed: int
    dead: int
    total: int


class QueuesResponse(BaseModel):
    """Response for queue listing."""
    queues: List[QueueStats]


def get_broker():
    """Get the shared broker instance."""
    from ..main import get_app_broker
    return get_app_broker()


@router.get("", response_model=QueuesResponse)
async def list_queues():
    """List all queues and their statistics."""
    broker = get_broker()
    queue_stats = broker.get_queue_stats()
    
    queues = [
        QueueStats(
            name=qs.name,
            pending=qs.pending,
            running=qs.running,
            completed=qs.completed,
            failed=qs.failed,
            dead=qs.dead,
            total=qs.total,
        )
        for qs in queue_stats.values()
    ]
    
    # Add default queue if not present
    if not any(q.name == "default" for q in queues):
        queues.insert(0, QueueStats(
            name="default",
            pending=0,
            running=0,
            completed=0,
            failed=0,
            dead=0,
            total=0,
        ))
    
    return QueuesResponse(queues=queues)


@router.get("/{queue_name}", response_model=QueueStats)
async def get_queue(queue_name: str):
    """Get statistics for a specific queue."""
    broker = get_broker()
    queue_stats = broker.get_queue_stats()
    
    if queue_name in queue_stats:
        qs = queue_stats[queue_name]
        return QueueStats(
            name=qs.name,
            pending=qs.pending,
            running=qs.running,
            completed=qs.completed,
            failed=qs.failed,
            dead=qs.dead,
            total=qs.total,
        )
    
    # Return empty stats for non-existent queue
    return QueueStats(
        name=queue_name,
        pending=0,
        running=0,
        completed=0,
        failed=0,
        dead=0,
        total=0,
    )
