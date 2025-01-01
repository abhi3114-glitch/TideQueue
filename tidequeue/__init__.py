"""
TideQueue - Distributed Background Worker Engine

A lightweight, production-ready task queue and worker system.
"""

from .models import Job, JobStatus, RetryPolicy, WorkerState
from .broker import Broker
from .worker import Worker
from .pool import WorkerPool
from .dlq import DeadLetterQueue
from .decorators import task
from .config import Config
from .exceptions import (
    TideQueueError,
    JobNotFoundError,
    QueueFullError,
    WorkerError,
    RetryExhaustedError,
)

__version__ = "1.0.0"
__all__ = [
    "Job",
    "JobStatus",
    "RetryPolicy",
    "WorkerState",
    "Broker",
    "Worker",
    "WorkerPool",
    "DeadLetterQueue",
    "task",
    "Config",
    "TideQueueError",
    "JobNotFoundError",
    "QueueFullError",
    "WorkerError",
    "RetryExhaustedError",
]


class TideQueue:
    """Main entry point for TideQueue operations."""
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.broker = Broker(self.config)
        self.pool = None
    
    def enqueue(self, task_name: str, args: list = None, kwargs: dict = None, 
                priority: int = 0, queue: str = "default") -> str:
        """Submit a job to the queue."""
        job = Job.create(
            name=task_name,
            args=args or [],
            kwargs=kwargs or {},
            priority=priority,
            queue=queue,
        )
        self.broker.enqueue(job)
        return job.id
    
    def get_job(self, job_id: str) -> Job:
        """Get job by ID."""
        return self.broker.get_job(job_id)
    
    def start_workers(self, count: int = None):
        """Start the worker pool."""
        count = count or self.config.worker_count
        self.pool = WorkerPool(self.broker, count)
        self.pool.start()
    
    def stop_workers(self):
        """Stop the worker pool."""
        if self.pool:
            self.pool.stop()
