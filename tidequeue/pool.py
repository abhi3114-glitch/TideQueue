"""
TideQueue Worker Pool Management
"""

import uuid
import logging
import threading
from typing import List, Dict, Optional

from .worker import Worker
from .broker import Broker
from .config import Config
from .models import WorkerStatus

logger = logging.getLogger(__name__)


class WorkerPool:
    """
    Manages a pool of workers.
    
    Provides dynamic scaling, health monitoring, and graceful shutdown.
    """
    
    def __init__(
        self,
        broker: Broker,
        size: int = 4,
        queues: list = None,
        config: Config = None,
    ):
        self.broker = broker
        self.size = size
        self.queues = queues or ["default"]
        self.config = config or Config()
        
        self._workers: Dict[str, Worker] = {}
        self._lock = threading.RLock()
        self._running = False
    
    def start(self):
        """Start all workers in the pool."""
        if self._running:
            logger.warning("Worker pool already running")
            return
        
        self._running = True
        
        with self._lock:
            for i in range(self.size):
                self._spawn_worker()
        
        logger.info(f"Worker pool started with {self.size} workers")
    
    def stop(self, timeout: float = 30.0):
        """Stop all workers gracefully."""
        if not self._running:
            return
        
        logger.info("Stopping worker pool...")
        self._running = False
        
        with self._lock:
            # Stop all workers
            per_worker_timeout = timeout / max(len(self._workers), 1)
            for worker in list(self._workers.values()):
                worker.stop(timeout=per_worker_timeout)
            
            self._workers.clear()
        
        logger.info("Worker pool stopped")
    
    def _spawn_worker(self) -> Worker:
        """Spawn a new worker and add it to the pool."""
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        
        worker = Worker(
            worker_id=worker_id,
            broker=self.broker,
            queues=self.queues,
            config=self.config,
        )
        
        self._workers[worker_id] = worker
        worker.start()
        
        logger.debug(f"Spawned worker {worker_id}")
        return worker
    
    def _remove_worker(self, worker_id: str, timeout: float = 10.0) -> bool:
        """Remove a worker from the pool."""
        with self._lock:
            worker = self._workers.get(worker_id)
            if worker:
                worker.stop(timeout=timeout)
                del self._workers[worker_id]
                self.broker.persistence.remove_worker(worker_id)
                logger.debug(f"Removed worker {worker_id}")
                return True
        return False
    
    def scale(self, new_size: int):
        """
        Scale the worker pool to a new size.
        
        Adds or removes workers as needed.
        """
        if new_size < 1:
            raise ValueError("Pool size must be at least 1")
        
        with self._lock:
            current_size = len(self._workers)
            
            if new_size > current_size:
                # Add workers
                for _ in range(new_size - current_size):
                    self._spawn_worker()
                logger.info(f"Scaled up from {current_size} to {new_size} workers")
            
            elif new_size < current_size:
                # Remove workers (prefer idle ones)
                workers_to_remove = current_size - new_size
                
                # Sort by idle status (idle first)
                idle_workers = [
                    w for w in self._workers.values()
                    if w.state.status == WorkerStatus.IDLE
                ]
                busy_workers = [
                    w for w in self._workers.values()
                    if w.state.status != WorkerStatus.IDLE
                ]
                
                # Remove idle workers first
                workers_list = idle_workers + busy_workers
                for worker in workers_list[:workers_to_remove]:
                    self._remove_worker(worker.id)
                
                logger.info(f"Scaled down from {current_size} to {new_size} workers")
            
            self.size = new_size
    
    def get_workers(self) -> List[Dict]:
        """Get status of all workers."""
        with self._lock:
            return [
                {
                    "id": w.id,
                    "status": w.state.status.value,
                    "current_job_id": w.state.current_job_id,
                    "jobs_completed": w.state.jobs_completed,
                    "jobs_failed": w.state.jobs_failed,
                    "started_at": w.state.started_at.isoformat(),
                    "last_heartbeat": w.state.last_heartbeat.isoformat(),
                    "queues": w.queues,
                }
                for w in self._workers.values()
            ]
    
    def get_stats(self) -> Dict:
        """Get pool statistics."""
        with self._lock:
            total = len(self._workers)
            idle = sum(1 for w in self._workers.values() if w.state.status == WorkerStatus.IDLE)
            busy = sum(1 for w in self._workers.values() if w.state.status == WorkerStatus.BUSY)
            
            total_completed = sum(w.state.jobs_completed for w in self._workers.values())
            total_failed = sum(w.state.jobs_failed for w in self._workers.values())
            
            return {
                "total_workers": total,
                "idle": idle,
                "busy": busy,
                "jobs_completed": total_completed,
                "jobs_failed": total_failed,
                "queues": self.queues,
            }
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    @property
    def worker_count(self) -> int:
        return len(self._workers)
