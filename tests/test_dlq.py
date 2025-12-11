"""
Tests for TideQueue Dead-Letter Queue
"""

import pytest
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tidequeue.dlq import DeadLetterQueue
from tidequeue.broker import Broker
from tidequeue.config import Config
from tidequeue.models import Job, JobStatus, RetryPolicy


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


@pytest.fixture
def dlq(broker):
    """Create a DLQ handler."""
    return DeadLetterQueue(broker.persistence)


class TestDeadLetterQueue:
    """Tests for dead-letter queue functionality."""
    
    def test_move_job_to_dlq(self, broker, dlq):
        """Test moving a job to the DLQ."""
        job = Job.create(name="test_task")
        broker.enqueue(job)
        broker.dequeue("default")
        
        dlq.move_job(job, "Max retries exceeded")
        
        dlq_jobs = dlq.get_jobs()
        assert len(dlq_jobs) == 1
        assert dlq_jobs[0]["id"] == job.id
        assert dlq_jobs[0]["reason"] == "Max retries exceeded"
    
    def test_get_dlq_job(self, broker, dlq):
        """Test getting a specific job from the DLQ."""
        job = Job.create(name="test_task")
        broker.enqueue(job)
        broker.dequeue("default")
        dlq.move_job(job, "Test reason")
        
        dlq_job = dlq.get_job(job.id)
        
        assert dlq_job is not None
        assert dlq_job["id"] == job.id
    
    def test_replay_dlq_job(self, broker, dlq):
        """Test replaying a job from the DLQ."""
        job = Job.create(name="test_task")
        job.error = "Previous error"
        job.retry_count = 5
        broker.enqueue(job)
        broker.dequeue("default")
        dlq.move_job(job, "Test reason")
        
        replayed = dlq.replay(job.id)
        
        assert replayed is not None
        assert replayed.status == JobStatus.PENDING
        assert replayed.error is None
        assert replayed.retry_count == 0  # Reset by default
        
        # Should be removed from DLQ
        assert dlq.get_job(job.id) is None
    
    def test_replay_dlq_job_keep_retry_count(self, broker, dlq):
        """Test replaying a job while keeping retry count."""
        job = Job.create(name="test_task")
        job.retry_count = 5
        broker.enqueue(job)
        broker.dequeue("default")
        dlq.move_job(job, "Test reason")
        
        replayed = dlq.replay(job.id, reset_retry_count=False)
        
        assert replayed.retry_count == 5
    
    def test_purge_dlq_job(self, broker, dlq):
        """Test purging a job from the DLQ."""
        job = Job.create(name="test_task")
        broker.enqueue(job)
        broker.dequeue("default")
        dlq.move_job(job, "Test reason")
        
        success = dlq.purge(job.id)
        
        assert success is True
        assert dlq.get_job(job.id) is None
    
    def test_purge_all_dlq(self, broker, dlq):
        """Test purging all jobs from the DLQ."""
        for i in range(5):
            job = Job.create(name=f"task_{i}")
            broker.enqueue(job)
            broker.dequeue("default")
            dlq.move_job(job, f"Reason {i}")
        
        count = dlq.purge_all()
        
        assert count == 5
        assert len(dlq.get_jobs()) == 0
    
    def test_get_dlq_stats(self, broker, dlq):
        """Test getting DLQ statistics."""
        # Add jobs with different task names and errors
        for i in range(3):
            job = Job.create(name="task_a")
            job.error = "Error Type A"
            broker.enqueue(job)
            broker.dequeue("default")
            dlq.move_job(job, "Reason")
        
        for i in range(2):
            job = Job.create(name="task_b")
            job.error = "Error Type B"
            broker.enqueue(job)
            broker.dequeue("default")
            dlq.move_job(job, "Reason")
        
        stats = dlq.get_stats()
        
        assert stats["total"] == 5
        assert stats["by_task"]["task_a"] == 3
        assert stats["by_task"]["task_b"] == 2
        assert "Error Type A" in stats["by_error"]
        assert "Error Type B" in stats["by_error"]


class TestDLQIntegration:
    """Integration tests for DLQ with broker."""
    
    def test_job_moved_to_dlq_after_max_retries(self, broker, dlq):
        """Test that job is moved to DLQ after exhausting retries."""
        policy = RetryPolicy(max_retries=2)
        job = Job.create(name="test_task", retry_policy=policy)
        broker.enqueue(job)
        
        # Simulate failures
        for _ in range(3):
            dequeued = broker.dequeue("default")
            if dequeued:
                broker.nack(dequeued.id, error="Test error")
        
        # Job should be in DLQ
        updated = broker.get_job(job.id)
        assert updated.status == JobStatus.DEAD
        
        dlq_jobs = dlq.get_jobs()
        assert any(j["id"] == job.id for j in dlq_jobs)
