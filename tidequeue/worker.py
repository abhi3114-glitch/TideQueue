"""
TideQueue Worker Implementation
"""

import os
import signal
import logging
import threading
import traceback
import time
from datetime import datetime
from typing import Optional, Callable, Any

from .models import Job, JobStatus, WorkerState, WorkerStatus
from .broker import Broker
from .decorators import TaskExecutor
from .retry import RetryEngine
from .config import Config

logger = logging.getLogger(__name__)


class Worker:
    """
    A worker that processes jobs from the queue.
    
    Each worker runs in its own thread and continuously polls for jobs.
    """
    
    def __init__(
        self,
        worker_id: str,
        broker: Broker,
        queues: list = None,
        config: Config = None,
    ):
        self.id = worker_id
        self.broker = broker
        self.queues = queues or ["default"]
        self.config = config or Config()
        
        self.executor = TaskExecutor()
        self.retry_engine = RetryEngine()
        
        self.state = WorkerState(id=worker_id)
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # Callbacks
        self._on_job_complete: Optional[Callable] = None
        self._on_job_failed: Optional[Callable] = None
    
    def start(self):
        """Start the worker in a background thread."""
        if self._running:
            return
        
        self._running = True
        self._stop_event.clear()
        
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        
        logger.info(f"Worker {self.id} started, listening on queues: {self.queues}")
    
    def stop(self, timeout: float = 10.0):
        """Stop the worker gracefully."""
        if not self._running:
            return
        
        logger.info(f"Worker {self.id} stopping...")
        self._running = False
        self._stop_event.set()
        
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                logger.warning(f"Worker {self.id} did not stop gracefully")
        
        self.state.status = WorkerStatus.STOPPED
        self.broker.persistence.update_worker_heartbeat(
            self.id, WorkerStatus.STOPPED.value
        )
        
        logger.info(f"Worker {self.id} stopped")
    
    def _run_loop(self):
        """Main worker loop."""
        while self._running and not self._stop_event.is_set():
            job = self._fetch_job()
            
            if job:
                self._process_job(job)
            else:
                # No job available, wait before polling again
                self._stop_event.wait(timeout=1.0)
            
            # Send heartbeat
            self._send_heartbeat()
    
    def _fetch_job(self) -> Optional[Job]:
        """Fetch the next job from any of our queues."""
        for queue in self.queues:
            job = self.broker.dequeue(queue, worker_id=self.id)
            if job:
                return job
        return None
    
    def _process_job(self, job: Job):
        """Process a single job."""
        logger.info(f"Worker {self.id} processing job {job.id} ({job.name})")
        
        self.state.status = WorkerStatus.BUSY
        self.state.current_job_id = job.id
        
        try:
            # Execute the task
            result = self.executor.execute(job)
            
            # Job completed successfully
            self.broker.ack(job.id, result)
            self.state.jobs_completed += 1
            
            logger.info(f"Job {job.id} completed successfully")
            
            if self._on_job_complete:
                self._on_job_complete(job, result)
        
        except Exception as e:
            # Job failed
            error_msg = str(e)
            error_tb = traceback.format_exc()
            
            logger.error(f"Job {job.id} failed: {error_msg}")
            
            # Handle retry logic
            should_retry = self.broker.nack(job.id, error_msg, error_tb)
            self.state.jobs_failed += 1
            
            if should_retry:
                # Calculate retry delay
                delay = self.retry_engine.get_next_delay(job)
                logger.info(f"Job {job.id} will retry in {delay:.1f}s")
            
            if self._on_job_failed:
                self._on_job_failed(job, e)
        
        finally:
            self.state.status = WorkerStatus.IDLE
            self.state.current_job_id = None
    
    def _send_heartbeat(self):
        """Send heartbeat to persistence layer."""
        self.state.last_heartbeat = datetime.utcnow()
        
        self.broker.persistence.update_worker_heartbeat(
            worker_id=self.id,
            status=self.state.status.value,
            current_job_id=self.state.current_job_id,
            jobs_completed=self.state.jobs_completed,
            jobs_failed=self.state.jobs_failed,
        )
    
    def on_job_complete(self, callback: Callable):
        """Register callback for job completion."""
        self._on_job_complete = callback
    
    def on_job_failed(self, callback: Callable):
        """Register callback for job failure."""
        self._on_job_failed = callback
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    @property
    def is_busy(self) -> bool:
        return self.state.status == WorkerStatus.BUSY
