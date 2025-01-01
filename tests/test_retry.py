"""
Tests for TideQueue Retry Engine
"""

import pytest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tidequeue.retry import RetryEngine
from tidequeue.models import Job, RetryPolicy, BackoffType


@pytest.fixture
def engine():
    """Create a retry engine."""
    return RetryEngine()


class TestRetryEngine:
    """Tests for retry engine functionality."""
    
    def test_should_retry_true(self, engine):
        """Test that should_retry returns True when retries remain."""
        policy = RetryPolicy(max_retries=3)
        job = Job.create(name="test_task", retry_policy=policy)
        job.retry_count = 1
        
        assert engine.should_retry(job) is True
    
    def test_should_retry_false(self, engine):
        """Test that should_retry returns False when retries exhausted."""
        policy = RetryPolicy(max_retries=3)
        job = Job.create(name="test_task", retry_policy=policy)
        job.retry_count = 3
        
        assert engine.should_retry(job) is False
    
    def test_constant_backoff(self, engine):
        """Test constant backoff returns same delay."""
        policy = RetryPolicy(
            max_retries=5,
            backoff_type=BackoffType.CONSTANT,
            initial_delay=5.0,
            jitter=False,
        )
        job = Job.create(name="test_task", retry_policy=policy)
        
        job.retry_count = 1
        delay1 = engine.get_next_delay(job)
        
        job.retry_count = 3
        delay3 = engine.get_next_delay(job)
        
        assert delay1 == 5.0
        assert delay3 == 5.0
    
    def test_linear_backoff(self, engine):
        """Test linear backoff increases linearly."""
        policy = RetryPolicy(
            max_retries=5,
            backoff_type=BackoffType.LINEAR,
            initial_delay=2.0,
            jitter=False,
        )
        job = Job.create(name="test_task", retry_policy=policy)
        
        job.retry_count = 1
        delay1 = engine.get_next_delay(job)
        
        job.retry_count = 2
        delay2 = engine.get_next_delay(job)
        
        job.retry_count = 3
        delay3 = engine.get_next_delay(job)
        
        assert delay1 == 2.0
        assert delay2 == 4.0
        assert delay3 == 6.0
    
    def test_exponential_backoff(self, engine):
        """Test exponential backoff doubles each time."""
        policy = RetryPolicy(
            max_retries=5,
            backoff_type=BackoffType.EXPONENTIAL,
            initial_delay=1.0,
            jitter=False,
        )
        job = Job.create(name="test_task", retry_policy=policy)
        
        job.retry_count = 1
        delay1 = engine.get_next_delay(job)
        
        job.retry_count = 2
        delay2 = engine.get_next_delay(job)
        
        job.retry_count = 3
        delay3 = engine.get_next_delay(job)
        
        assert delay1 == 1.0
        assert delay2 == 2.0
        assert delay3 == 4.0
    
    def test_fibonacci_backoff(self, engine):
        """Test fibonacci backoff follows fibonacci sequence."""
        policy = RetryPolicy(
            max_retries=7,
            backoff_type=BackoffType.FIBONACCI,
            initial_delay=1.0,
            jitter=False,
        )
        job = Job.create(name="test_task", retry_policy=policy)
        
        # Fibonacci: 0, 1, 1, 2, 3, 5, 8
        expected = [1, 1, 2, 3, 5]
        
        for i, expected_mult in enumerate(expected, start=1):
            job.retry_count = i
            delay = engine.get_next_delay(job)
            assert delay == expected_mult * 1.0
    
    def test_max_delay_cap(self, engine):
        """Test that delay is capped at max_delay."""
        policy = RetryPolicy(
            max_retries=10,
            backoff_type=BackoffType.EXPONENTIAL,
            initial_delay=1.0,
            max_delay=10.0,
            jitter=False,
        )
        job = Job.create(name="test_task", retry_policy=policy)
        
        job.retry_count = 8  # Would be 128 without cap
        delay = engine.get_next_delay(job)
        
        assert delay == 10.0
    
    def test_jitter_varies_delay(self, engine):
        """Test that jitter adds variance to delay."""
        policy = RetryPolicy(
            max_retries=5,
            backoff_type=BackoffType.CONSTANT,
            initial_delay=10.0,
            jitter=True,
        )
        job = Job.create(name="test_task", retry_policy=policy)
        job.retry_count = 1
        
        delays = set()
        for _ in range(10):
            delays.add(engine.get_next_delay(job))
        
        # With jitter, we should get different values
        assert len(delays) > 1
    
    def test_handle_failure_should_retry(self, engine):
        """Test handle_failure when should retry."""
        policy = RetryPolicy(max_retries=3)
        job = Job.create(name="test_task", retry_policy=policy)
        job.retry_count = 0
        
        result = engine.handle_failure(job, "Test error")
        
        assert result["should_retry"] is True
        assert result["delay"] > 0
        assert result["retry_at"] is not None
        assert "retry" in result["reason"].lower()
    
    def test_handle_failure_no_retry(self, engine):
        """Test handle_failure when retries exhausted."""
        policy = RetryPolicy(max_retries=3)
        job = Job.create(name="test_task", retry_policy=policy)
        job.retry_count = 3
        
        result = engine.handle_failure(job, "Final error")
        
        assert result["should_retry"] is False
        assert result["delay"] == 0
        assert result["retry_at"] is None
        assert "dlq" in result["reason"].lower()


class TestRetryPolicyCreation:
    """Tests for creating retry policies."""
    
    def test_create_policy(self, engine):
        """Test creating a retry policy via factory method."""
        policy = RetryEngine.create_policy(
            max_retries=5,
            backoff="fibonacci",
            initial_delay=2.0,
        )
        
        assert policy.max_retries == 5
        assert policy.backoff_type == BackoffType.FIBONACCI
        assert policy.initial_delay == 2.0
    
    def test_policy_defaults(self):
        """Test default retry policy values."""
        policy = RetryPolicy()
        
        assert policy.max_retries == 3
        assert policy.backoff_type == BackoffType.EXPONENTIAL
        assert policy.initial_delay == 1.0
        assert policy.jitter is True
