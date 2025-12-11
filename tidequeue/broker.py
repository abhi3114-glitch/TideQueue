"""
TideQueue Message Broker - In-memory queue with persistence
"""

import heapq
import threading
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from .models import Job, JobStatus, QueueStats
from .config import Config
from .persistence import PersistenceLayer
from .exceptions import JobNotFoundError, QueueFullError

logger = logging.getLogger(__name__)


class Broker:
    """
    Thread-safe message broker with priority queue support.
    
    Jobs are stored both in-memory (for fast access) and persisted to SQLite
    for durability across restarts.
    """
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.persistence = PersistenceLayer(self.config)
        
        # In-memory priority queues: queue_name -> list of (priority, created_at, job)
        self._queues: Dict[str, list] = {}
        self._lock = threading.RLock()
        
        # Job lookup cache
        self._jobs: Dict[str, Job] = {}
        
        # Statistics
        self._stats = {
            "enqueued": 0,
            "dequeued": 0,
            "completed": 0,
            "failed": 0,
        }
        
        # Recover jobs from persistence on startup
        self._recover_jobs()
    
    def _recover_jobs(self):
        """Recover pending and running jobs from persistence."""
        logger.info("Recovering jobs from persistence...")
        
        # Recover pending jobs
        pending_jobs = self.persistence.get_jobs_by_status(JobStatus.PENDING, limit=10000)
        for job in pending_jobs:
            self._add_to_queue(job)
        
        # Reset running jobs back to pending (worker may have crashed)
        running_jobs = self.persistence.get_jobs_by_status(JobStatus.RUNNING, limit=10000)
        for job in running_jobs:
            job.status = JobStatus.PENDING
            job.worker_id = None
            job.started_at = None
            self.persistence.save_job(job)
            self._add_to_queue(job)
        
        logger.info(f"Recovered {len(pending_jobs)} pending and {len(running_jobs)} running jobs")
    
    def _add_to_queue(self, job: Job):
        """Add job to in-memory priority queue."""
        queue_name = job.queue
        if queue_name not in self._queues:
            self._queues[queue_name] = []
        
        # Use negative priority for max-heap behavior (higher priority first)
        entry = (-job.priority, job.created_at, job.id)
        heapq.heappush(self._queues[queue_name], entry)
        self._jobs[job.id] = job
    
    def enqueue(self, job: Job) -> str:
        """
        Add a job to the queue.
        
        Returns the job ID.
        """
        with self._lock:
            # Check queue capacity
            queue_name = job.queue
            if queue_name in self._queues:
                if len(self._queues[queue_name]) >= self.config.max_queue_size:
                    raise QueueFullError(queue_name, self.config.max_queue_size)
            
            # Persist first
            self.persistence.save_job(job)
            
            # Add to in-memory queue
            self._add_to_queue(job)
            
            self._stats["enqueued"] += 1
            logger.debug(f"Enqueued job {job.id} ({job.name}) to queue '{queue_name}'")
            
            return job.id
    
    def dequeue(self, queue: str = "default", worker_id: str = None) -> Optional[Job]:
        """
        Get the next job from the queue.
        
        Returns None if the queue is empty.
        """
        with self._lock:
            if queue not in self._queues or not self._queues[queue]:
                return None
            
            # Pop highest priority job
            while self._queues[queue]:
                _, _, job_id = heapq.heappop(self._queues[queue])
                
                # Check if job is still pending (might have been cancelled)
                if job_id in self._jobs:
                    job = self._jobs[job_id]
                    if job.status == JobStatus.PENDING:
                        # Mark as running
                        job.status = JobStatus.RUNNING
                        job.started_at = datetime.utcnow()
                        job.worker_id = worker_id
                        
                        self.persistence.save_job(job)
                        self._stats["dequeued"] += 1
                        
                        logger.debug(f"Dequeued job {job.id} ({job.name}) for worker {worker_id}")
                        return job
            
            return None
    
    def ack(self, job_id: str, result: Any = None) -> None:
        """
        Acknowledge successful job completion.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise JobNotFoundError(job_id)
            
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.result = result
            
            self.persistence.save_job(job)
            self._stats["completed"] += 1
            
            logger.debug(f"Acknowledged job {job_id} as completed")
    
    def nack(self, job_id: str, error: str = None, traceback: str = None) -> bool:
        """
        Negative acknowledge - job failed.
        
        Returns True if job should be retried, False if moved to DLQ.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise JobNotFoundError(job_id)
            
            job.error = error
            job.traceback = traceback
            job.retry_count += 1
            
            # Check if we should retry
            if job.retry_count <= job.retry_policy.max_retries:
                # Schedule for retry
                job.status = JobStatus.PENDING
                job.started_at = None
                job.worker_id = None
                
                self.persistence.save_job(job)
                self._add_to_queue(job)
                
                self._stats["failed"] += 1
                logger.info(f"Job {job_id} failed, retry {job.retry_count}/{job.retry_policy.max_retries}")
                return True
            else:
                # Move to DLQ
                job.status = JobStatus.DEAD
                self.persistence.move_to_dlq(job, f"Exhausted {job.retry_policy.max_retries} retries")
                
                self._stats["failed"] += 1
                logger.warning(f"Job {job_id} moved to DLQ after exhausting retries")
                return False
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID."""
        with self._lock:
            if job_id in self._jobs:
                return self._jobs[job_id]
            return self.persistence.get_job(job_id)
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                job = self.persistence.get_job(job_id)
            
            if not job:
                raise JobNotFoundError(job_id)
            
            if job.status not in (JobStatus.PENDING, JobStatus.SCHEDULED):
                return False
            
            job.status = JobStatus.CANCELLED
            job.completed_at = datetime.utcnow()
            self.persistence.save_job(job)
            
            # Remove from in-memory cache
            if job_id in self._jobs:
                del self._jobs[job_id]
            
            logger.info(f"Cancelled job {job_id}")
            return True
    
    def retry_job(self, job_id: str) -> bool:
        """Retry a failed job."""
        with self._lock:
            job = self.persistence.get_job(job_id)
            if not job:
                raise JobNotFoundError(job_id)
            
            if job.status not in (JobStatus.FAILED, JobStatus.DEAD, JobStatus.CANCELLED):
                return False
            
            # Reset job state
            job.status = JobStatus.PENDING
            job.result = None
            job.error = None
            job.traceback = None
            job.started_at = None
            job.completed_at = None
            job.worker_id = None
            # Keep retry_count to track total attempts
            
            self.persistence.save_job(job)
            self._add_to_queue(job)
            
            logger.info(f"Retrying job {job_id}")
            return True
    
    def get_queue_stats(self) -> Dict[str, QueueStats]:
        """Get statistics for all queues."""
        return self.persistence.get_queue_stats()
    
    def get_all_jobs(self, limit: int = 100, offset: int = 0) -> List[Job]:
        """Get all jobs with pagination."""
        return self.persistence.get_all_jobs(limit, offset)
    
    def get_jobs_by_status(self, status: JobStatus, limit: int = 100) -> List[Job]:
        """Get jobs by status."""
        return self.persistence.get_jobs_by_status(status, limit)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get broker statistics."""
        queue_stats = self.get_queue_stats()
        
        total_pending = sum(qs.pending for qs in queue_stats.values())
        total_running = sum(qs.running for qs in queue_stats.values())
        total_completed = sum(qs.completed for qs in queue_stats.values())
        total_failed = sum(qs.failed for qs in queue_stats.values())
        total_dead = sum(qs.dead for qs in queue_stats.values())
        
        return {
            "queues": len(queue_stats),
            "pending": total_pending,
            "running": total_running,
            "completed": total_completed,
            "failed": total_failed,
            "dead": total_dead,
            "total": total_pending + total_running + total_completed + total_failed + total_dead,
            "throughput": self._stats,
        }
    
    def close(self):
        """Close the broker and persistence layer."""
        self.persistence.close()
