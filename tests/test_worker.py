"""
Tests for TideQueue Worker and Pool
"""

import pytest
import os
import sys
import time
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tidequeue.worker import Worker
from tidequeue.pool import WorkerPool
from tidequeue.broker import Broker
from tidequeue.config import Config
from tidequeue.models import Job, WorkerStatus
from tidequeue.decorators import task, get_task_registry


# Register a test task
@task(queue="default")
def test_task(x, y):
    return x + y


@task(queue="default")
def slow_task(duration):
    time.sleep(duration)
    return "done"


@task(queue="default")
def failing_task():
    raise Exception("Task failed!")


@pytest.fixture
def temp_db():
    """Create a temporary database file."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def broker(temp_db):
    """Create a broker with temporary database."""
    config = Config(db_path=temp_db)
    broker = Broker(config)
    yield broker
    broker.close()


class TestWorker:
    """Tests for individual workers."""
    
    def test_worker_creation(self, broker):
        """Test worker creation."""
        worker = Worker("worker-1", broker)
        
        assert worker.id == "worker-1"
        assert worker.state.status == WorkerStatus.IDLE
        assert not worker.is_running
    
    def test_worker_start_stop(self, broker):
        """Test worker start and stop."""
        worker = Worker("worker-1", broker)
        
        worker.start()
        assert worker.is_running
        
        worker.stop(timeout=2)
        assert not worker.is_running
        assert worker.state.status == WorkerStatus.STOPPED
    
    def test_worker_processes_job(self, broker):
        """Test that worker processes a job."""
        job = Job.create(name="test_task", args=[1, 2])
        broker.enqueue(job)
        
        worker = Worker("worker-1", broker)
        worker.start()
        
        # Wait for job to be processed
        time.sleep(0.5)
        
        worker.stop(timeout=2)
        
        updated = broker.get_job(job.id)
        assert updated.result == 3
    
    def test_worker_handles_failed_job(self, broker):
        """Test that worker handles job failure."""
        job = Job.create(name="failing_task")
        broker.enqueue(job)
        
        worker = Worker("worker-1", broker)
        worker.start()
        
        time.sleep(0.5)
        
        worker.stop(timeout=2)
        
        assert worker.state.jobs_failed >= 1
    
    def test_worker_tracks_completed_jobs(self, broker):
        """Test that worker tracks completed job count."""
        for i in range(3):
            job = Job.create(name="test_task", args=[i, i])
            broker.enqueue(job)
        
        worker = Worker("worker-1", broker)
        worker.start()
        
        time.sleep(1)
        
        worker.stop(timeout=2)
        
        assert worker.state.jobs_completed >= 3


class TestWorkerPool:
    """Tests for worker pool."""
    
    def test_pool_creation(self, broker):
        """Test pool creation."""
        pool = WorkerPool(broker, size=4)
        
        assert pool.size == 4
        assert not pool.is_running
    
    def test_pool_start_stop(self, broker):
        """Test pool start and stop."""
        pool = WorkerPool(broker, size=2)
        
        pool.start()
        assert pool.is_running
        assert pool.worker_count == 2
        
        pool.stop(timeout=5)
        assert not pool.is_running
        assert pool.worker_count == 0
    
    def test_pool_scale_up(self, broker):
        """Test scaling pool up."""
        pool = WorkerPool(broker, size=2)
        pool.start()
        
        assert pool.worker_count == 2
        
        pool.scale(4)
        
        assert pool.worker_count == 4
        
        pool.stop(timeout=5)
    
    def test_pool_scale_down(self, broker):
        """Test scaling pool down."""
        pool = WorkerPool(broker, size=4)
        pool.start()
        
        assert pool.worker_count == 4
        
        pool.scale(2)
        
        assert pool.worker_count == 2
        
        pool.stop(timeout=5)
    
    def test_pool_get_workers(self, broker):
        """Test getting worker list."""
        pool = WorkerPool(broker, size=2)
        pool.start()
        
        workers = pool.get_workers()
        
        assert len(workers) == 2
        assert all("id" in w for w in workers)
        assert all("status" in w for w in workers)
        
        pool.stop(timeout=5)
    
    def test_pool_get_stats(self, broker):
        """Test getting pool statistics."""
        pool = WorkerPool(broker, size=2)
        pool.start()
        
        stats = pool.get_stats()
        
        assert stats["total_workers"] == 2
        assert "idle" in stats
        assert "busy" in stats
        assert "jobs_completed" in stats
        
        pool.stop(timeout=5)
    
    def test_pool_processes_jobs(self, broker):
        """Test that pool processes multiple jobs."""
        for i in range(5):
            job = Job.create(name="test_task", args=[i, i])
            broker.enqueue(job)
        
        pool = WorkerPool(broker, size=2)
        pool.start()
        
        time.sleep(2)
        
        stats = pool.get_stats()
        
        pool.stop(timeout=5)
        
        assert stats["jobs_completed"] >= 5
    
    def test_pool_with_multiple_queues(self, broker):
        """Test pool with multiple queues."""
        broker.enqueue(Job.create(name="test_task", args=[1, 2], queue="default"))
        broker.enqueue(Job.create(name="test_task", args=[3, 4], queue="emails"))
        
        pool = WorkerPool(broker, size=2, queues=["default", "emails"])
        pool.start()
        
        time.sleep(1)
        
        pool.stop(timeout=5)
        
        stats = pool.get_stats()
        assert stats["jobs_completed"] >= 2
