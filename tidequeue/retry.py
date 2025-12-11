"""
TideQueue Retry Policy Engine
"""

import random
import math
import logging
from datetime import datetime, timedelta
from typing import Optional

from .models import Job, JobStatus, RetryPolicy, BackoffType

logger = logging.getLogger(__name__)


class RetryEngine:
    """
    Handles retry logic with configurable backoff strategies.
    """
    
    # Fibonacci sequence for fibonacci backoff
    _fib_cache = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610]
    
    def __init__(self):
        pass
    
    def should_retry(self, job: Job) -> bool:
        """
        Determine if a job should be retried.
        """
        return job.retry_count < job.retry_policy.max_retries
    
    def get_next_delay(self, job: Job) -> float:
        """
        Calculate the delay before the next retry attempt.
        
        Returns delay in seconds.
        """
        policy = job.retry_policy
        attempt = job.retry_count  # Already incremented when nack'd
        
        delay = self._calculate_base_delay(policy, attempt)
        
        # Apply jitter if enabled
        if policy.jitter:
            delay = self._apply_jitter(delay)
        
        # Cap at max delay
        delay = min(delay, policy.max_delay)
        
        return delay
    
    def _calculate_base_delay(self, policy: RetryPolicy, attempt: int) -> float:
        """Calculate base delay based on backoff type."""
        if policy.backoff_type == BackoffType.CONSTANT:
            return policy.initial_delay
        
        elif policy.backoff_type == BackoffType.LINEAR:
            return policy.initial_delay * attempt
        
        elif policy.backoff_type == BackoffType.EXPONENTIAL:
            return policy.initial_delay * (2 ** (attempt - 1))
        
        elif policy.backoff_type == BackoffType.FIBONACCI:
            if attempt < len(self._fib_cache):
                fib = self._fib_cache[attempt]
            else:
                # Calculate fibonacci for larger attempts
                fib = self._fibonacci(attempt)
            return policy.initial_delay * max(1, fib)
        
        return policy.initial_delay
    
    def _fibonacci(self, n: int) -> int:
        """Calculate fibonacci number for n."""
        if n <= 1:
            return n
        a, b = 0, 1
        for _ in range(2, n + 1):
            a, b = b, a + b
        return b
    
    def _apply_jitter(self, delay: float) -> float:
        """Apply random jitter to prevent thundering herd."""
        # Add +/- 25% jitter
        jitter_range = delay * 0.25
        jitter = random.uniform(-jitter_range, jitter_range)
        return max(0.1, delay + jitter)  # Minimum 100ms delay
    
    def get_retry_at(self, job: Job) -> datetime:
        """
        Calculate when the job should be retried.
        """
        delay = self.get_next_delay(job)
        return datetime.utcnow() + timedelta(seconds=delay)
    
    def handle_failure(self, job: Job, error: str, traceback: str = None) -> dict:
        """
        Handle a job failure and determine next steps.
        
        Returns a dict with:
        - should_retry: bool
        - delay: float (seconds before retry)
        - retry_at: datetime (when to retry)
        - reason: str (explanation)
        """
        job.error = error
        job.traceback = traceback
        
        if self.should_retry(job):
            delay = self.get_next_delay(job)
            retry_at = datetime.utcnow() + timedelta(seconds=delay)
            
            return {
                "should_retry": True,
                "delay": delay,
                "retry_at": retry_at,
                "reason": f"Retry {job.retry_count + 1}/{job.retry_policy.max_retries} in {delay:.1f}s",
            }
        else:
            return {
                "should_retry": False,
                "delay": 0,
                "retry_at": None,
                "reason": f"Max retries ({job.retry_policy.max_retries}) exhausted, moving to DLQ",
            }
    
    @staticmethod
    def create_policy(
        max_retries: int = 3,
        backoff: str = "exponential",
        initial_delay: float = 1.0,
        max_delay: float = 3600.0,
        jitter: bool = True,
    ) -> RetryPolicy:
        """
        Factory method to create a RetryPolicy.
        """
        backoff_type = BackoffType(backoff.lower())
        return RetryPolicy(
            max_retries=max_retries,
            backoff_type=backoff_type,
            initial_delay=initial_delay,
            max_delay=max_delay,
            jitter=jitter,
        )
