"""
TideQueue Dead-Letter Queue Handler
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from .models import Job, JobStatus
from .persistence import PersistenceLayer
from .config import Config

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """
    Handles jobs that have exhausted all retry attempts.
    
    Provides inspection, replay, and purge functionality.
    """
    
    def __init__(self, persistence: PersistenceLayer):
        self.persistence = persistence
    
    def move_job(self, job: Job, reason: str) -> None:
        """
        Move a failed job to the dead-letter queue.
        """
        logger.warning(f"Moving job {job.id} ({job.name}) to DLQ: {reason}")
        
        job.status = JobStatus.DEAD
        self.persistence.move_to_dlq(job, reason)
    
    def get_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get jobs in the DLQ.
        
        Returns list of dicts with job, reason, and moved_at.
        """
        return self.persistence.get_dlq_jobs(limit)
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific job from the DLQ.
        """
        jobs = self.persistence.get_dlq_jobs(limit=1000)
        for job_entry in jobs:
            if job_entry["id"] == job_id:
                return job_entry
        return None
    
    def replay(self, job_id: str, reset_retry_count: bool = True) -> Optional[Job]:
        """
        Replay a job from the DLQ.
        
        Removes from DLQ and returns the job for re-enqueueing.
        """
        job = self.persistence.remove_from_dlq(job_id)
        if job:
            # Reset job state for replay
            job.status = JobStatus.PENDING
            job.error = None
            job.traceback = None
            job.started_at = None
            job.completed_at = None
            job.worker_id = None
            
            if reset_retry_count:
                job.retry_count = 0
            
            logger.info(f"Replaying job {job_id} from DLQ")
            return job
        
        logger.warning(f"Job {job_id} not found in DLQ")
        return None
    
    def purge(self, job_id: str) -> bool:
        """
        Permanently remove a job from the DLQ.
        """
        job = self.persistence.remove_from_dlq(job_id)
        if job:
            # Also delete from main jobs table
            self.persistence.delete_job(job_id)
            logger.info(f"Purged job {job_id} from DLQ")
            return True
        return False
    
    def purge_all(self) -> int:
        """
        Purge all jobs from the DLQ.
        
        Returns the number of jobs purged.
        """
        jobs = self.persistence.get_dlq_jobs(limit=10000)
        count = 0
        for job_entry in jobs:
            if self.purge(job_entry["id"]):
                count += 1
        
        logger.info(f"Purged {count} jobs from DLQ")
        return count
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get DLQ statistics.
        """
        jobs = self.persistence.get_dlq_jobs(limit=10000)
        
        # Group by task name
        by_task = {}
        for job_entry in jobs:
            task_name = job_entry["job"].name
            if task_name not in by_task:
                by_task[task_name] = 0
            by_task[task_name] += 1
        
        # Group by error type
        by_error = {}
        for job_entry in jobs:
            error = job_entry["job"].error or "Unknown"
            error_type = error.split(":")[0] if ":" in error else error[:50]
            if error_type not in by_error:
                by_error[error_type] = 0
            by_error[error_type] += 1
        
        return {
            "total": len(jobs),
            "by_task": by_task,
            "by_error": by_error,
        }
