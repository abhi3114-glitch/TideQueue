"""
Tests for TideQueue Broker
"""

import pytest
import os
import tempfile

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tidequeue.broker import Broker
from tidequeue.config import Config
from tidequeue.models import Job, JobStatus, RetryPolicy
from tidequeue.exceptions import JobNotFoundError, QueueFullError


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


class TestBrokerEnqueue:
    """Tests for enqueueing jobs."""
    
    def test_enqueue_job(self, broker):
        """Test basic job enqueueing."""
        job = Job.create(name="test_task", args=[1, 2, 3])
        job_id = broker.enqueue(job)
        
        assert job_id == job.id
        assert job.status == JobStatus.PENDING
    
    def test_enqueue_with_priority(self, broker):
        """Test job enqueueing with different priorities."""
        low = Job.create(name="low_priority", priority=0)
        high = Job.create(name="high_priority", priority=10)
        
        broker.enqueue(low)
        broker.enqueue(high)
        
        # High priority should be dequeued first
        dequeued = broker.dequeue("default")
        assert dequeued.name == "high_priority"
    
    def test_enqueue_to_custom_queue(self, broker):
        """Test enqueueing to a custom queue."""
        job = Job.create(name="custom_task", queue="custom")
        broker.enqueue(job)
        
        # Should not be in default queue
        assert broker.dequeue("default") is None
        
        # Should be in custom queue
        dequeued = broker.dequeue("custom")
        assert dequeued.name == "custom_task"


class TestBrokerDequeue:
    """Tests for dequeueing jobs."""
    
    def test_dequeue_empty_queue(self, broker):
        """Test dequeueing from empty queue."""
        result = broker.dequeue("default")
        assert result is None
    
    def test_dequeue_updates_status(self, broker):
        """Test that dequeue updates job status to RUNNING."""
        job = Job.create(name="test_task")
        broker.enqueue(job)
        
        dequeued = broker.dequeue("default", worker_id="worker-1")
        
        assert dequeued.status == JobStatus.RUNNING
        assert dequeued.worker_id == "worker-1"
        assert dequeued.started_at is not None
    
    def test_dequeue_respects_priority(self, broker):
        """Test that higher priority jobs are dequeued first."""
        jobs = [
            Job.create(name="p1", priority=1),
            Job.create(name="p3", priority=3),
            Job.create(name="p2", priority=2),
        ]
        
        for job in jobs:
            broker.enqueue(job)
        
        # Should get p3 first, then p2, then p1
        assert broker.dequeue("default").name == "p3"
        assert broker.dequeue("default").name == "p2"
        assert broker.dequeue("default").name == "p1"


class TestBrokerAckNack:
    """Tests for acknowledging and negative-acknowledging jobs."""
    
    def test_ack_job(self, broker):
        """Test acknowledging a completed job."""
        job = Job.create(name="test_task")
        broker.enqueue(job)
        dequeued = broker.dequeue("default")
        
        broker.ack(dequeued.id, result={"success": True})
        
        updated = broker.get_job(dequeued.id)
        assert updated.status == JobStatus.COMPLETED
        assert updated.result == {"success": True}
        assert updated.completed_at is not None
    
    def test_nack_job_with_retry(self, broker):
        """Test negative acknowledgment with retry."""
        policy = RetryPolicy(max_retries=3)
        job = Job.create(name="test_task", retry_policy=policy)
        broker.enqueue(job)
        dequeued = broker.dequeue("default")
        
        should_retry = broker.nack(dequeued.id, error="Test error")
        
        assert should_retry is True
        updated = broker.get_job(dequeued.id)
        assert updated.status == JobStatus.PENDING
        assert updated.retry_count == 1
        assert updated.error == "Test error"
    
    def test_nack_job_exhausted_retries(self, broker):
        """Test negative acknowledgment when retries exhausted."""
        policy = RetryPolicy(max_retries=0)
        job = Job.create(name="test_task", retry_policy=policy)
        broker.enqueue(job)
        dequeued = broker.dequeue("default")
        
        should_retry = broker.nack(dequeued.id, error="Final error")
        
        assert should_retry is False
        updated = broker.get_job(dequeued.id)
        assert updated.status == JobStatus.DEAD


class TestBrokerJobManagement:
    """Tests for job management operations."""
    
    def test_get_job(self, broker):
        """Test getting a job by ID."""
        job = Job.create(name="test_task")
        broker.enqueue(job)
        
        retrieved = broker.get_job(job.id)
        
        assert retrieved.id == job.id
        assert retrieved.name == job.name
    
    def test_cancel_pending_job(self, broker):
        """Test cancelling a pending job."""
        job = Job.create(name="test_task")
        broker.enqueue(job)
        
        success = broker.cancel_job(job.id)
        
        assert success is True
        updated = broker.get_job(job.id)
        assert updated.status == JobStatus.CANCELLED
    
    def test_cannot_cancel_running_job(self, broker):
        """Test that running jobs cannot be cancelled."""
        job = Job.create(name="test_task")
        broker.enqueue(job)
        broker.dequeue("default")
        
        success = broker.cancel_job(job.id)
        
        assert success is False
    
    def test_retry_failed_job(self, broker):
        """Test retrying a failed job."""
        job = Job.create(name="test_task")
        broker.enqueue(job)
        broker.dequeue("default")
        broker.nack(job.id, error="Error")
        
        # Exhaust retries
        for _ in range(10):
            if broker.get_job(job.id).status == JobStatus.DEAD:
                break
            j = broker.dequeue("default")
            if j:
                broker.nack(j.id, error="Error")
        
        success = broker.retry_job(job.id)
        
        assert success is True
        updated = broker.get_job(job.id)
        assert updated.status == JobStatus.PENDING


class TestBrokerStats:
    """Tests for broker statistics."""
    
    def test_get_stats(self, broker):
        """Test getting broker statistics."""
        # Add some jobs
        for i in range(3):
            job = Job.create(name=f"task_{i}")
            broker.enqueue(job)
        
        stats = broker.get_stats()
        
        assert stats["pending"] == 3
        assert stats["queues"] >= 1
        assert "throughput" in stats
    
    def test_get_queue_stats(self, broker):
        """Test getting queue-specific statistics."""
        Job.create(name="default_task", queue="default")
        broker.enqueue(Job.create(name="default_task", queue="default"))
        broker.enqueue(Job.create(name="custom_task", queue="custom"))
        
        queue_stats = broker.get_queue_stats()
        
        assert "default" in queue_stats
        assert "custom" in queue_stats
        assert queue_stats["default"].pending == 1
        assert queue_stats["custom"].pending == 1
