"""
TideQueue Data Models
"""

import uuid
import json
from datetime import datetime
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Optional, List, Dict


class JobStatus(str, Enum):
    """Job execution status."""
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"  # Moved to DLQ
    CANCELLED = "cancelled"


class BackoffType(str, Enum):
    """Retry backoff strategies."""
    CONSTANT = "constant"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"


@dataclass
class RetryPolicy:
    """Configuration for job retry behavior."""
    max_retries: int = 3
    backoff_type: BackoffType = BackoffType.EXPONENTIAL
    initial_delay: float = 1.0  # seconds
    max_delay: float = 3600.0  # 1 hour max
    jitter: bool = True  # Add randomness to prevent thundering herd
    
    def to_dict(self) -> dict:
        return {
            "max_retries": self.max_retries,
            "backoff_type": self.backoff_type.value,
            "initial_delay": self.initial_delay,
            "max_delay": self.max_delay,
            "jitter": self.jitter,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "RetryPolicy":
        return cls(
            max_retries=data.get("max_retries", 3),
            backoff_type=BackoffType(data.get("backoff_type", "exponential")),
            initial_delay=data.get("initial_delay", 1.0),
            max_delay=data.get("max_delay", 3600.0),
            jitter=data.get("jitter", True),
        )


@dataclass
class Job:
    """Represents a task to be executed."""
    id: str
    name: str
    args: List[Any]
    kwargs: Dict[str, Any]
    queue: str = "default"
    priority: int = 0  # Higher = more priority
    status: JobStatus = JobStatus.PENDING
    retry_count: int = 0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    result: Optional[Any] = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    worker_id: Optional[str] = None
    
    @classmethod
    def create(cls, name: str, args: list = None, kwargs: dict = None,
               queue: str = "default", priority: int = 0,
               retry_policy: RetryPolicy = None) -> "Job":
        """Factory method to create a new job."""
        return cls(
            id=str(uuid.uuid4()),
            name=name,
            args=args or [],
            kwargs=kwargs or {},
            queue=queue,
            priority=priority,
            retry_policy=retry_policy or RetryPolicy(),
        )
    
    def to_dict(self) -> dict:
        """Serialize job to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "args": self.args,
            "kwargs": self.kwargs,
            "queue": self.queue,
            "priority": self.priority,
            "status": self.status.value,
            "retry_count": self.retry_count,
            "retry_policy": self.retry_policy.to_dict(),
            "result": self.result,
            "error": self.error,
            "traceback": self.traceback,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "worker_id": self.worker_id,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        """Deserialize job from dictionary."""
        return cls(
            id=data["id"],
            name=data["name"],
            args=data.get("args", []),
            kwargs=data.get("kwargs", {}),
            queue=data.get("queue", "default"),
            priority=data.get("priority", 0),
            status=JobStatus(data.get("status", "pending")),
            retry_count=data.get("retry_count", 0),
            retry_policy=RetryPolicy.from_dict(data.get("retry_policy", {})),
            result=data.get("result"),
            error=data.get("error"),
            traceback=data.get("traceback"),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.utcnow(),
            scheduled_at=datetime.fromisoformat(data["scheduled_at"]) if data.get("scheduled_at") else None,
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            worker_id=data.get("worker_id"),
        )
    
    def to_json(self) -> str:
        """Serialize job to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> "Job":
        """Deserialize job from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    def __lt__(self, other: "Job") -> bool:
        """Compare jobs by priority (for heap queue)."""
        # Higher priority first, then earlier created_at
        if self.priority != other.priority:
            return self.priority > other.priority
        return self.created_at < other.created_at


class WorkerStatus(str, Enum):
    """Worker status."""
    IDLE = "idle"
    BUSY = "busy"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class WorkerState:
    """Represents the state of a worker."""
    id: str
    status: WorkerStatus = WorkerStatus.IDLE
    current_job_id: Optional[str] = None
    jobs_completed: int = 0
    jobs_failed: int = 0
    started_at: datetime = field(default_factory=datetime.utcnow)
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "status": self.status.value,
            "current_job_id": self.current_job_id,
            "jobs_completed": self.jobs_completed,
            "jobs_failed": self.jobs_failed,
            "started_at": self.started_at.isoformat(),
            "last_heartbeat": self.last_heartbeat.isoformat(),
        }


@dataclass
class QueueStats:
    """Statistics for a queue."""
    name: str
    pending: int = 0
    running: int = 0
    completed: int = 0
    failed: int = 0
    dead: int = 0
    
    @property
    def total(self) -> int:
        return self.pending + self.running + self.completed + self.failed + self.dead
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "pending": self.pending,
            "running": self.running,
            "completed": self.completed,
            "failed": self.failed,
            "dead": self.dead,
            "total": self.total,
        }
