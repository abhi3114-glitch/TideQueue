"""
TideQueue Advanced Retry Example

This example demonstrates retry policies with different backoff strategies.
"""

import time
import random
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tidequeue import TideQueue, task, RetryPolicy, WorkerPool
from tidequeue.config import Config
from tidequeue.models import BackoffType


# Exponential backoff (default)
@task(
    queue="default",
    retry=RetryPolicy(
        max_retries=3,
        backoff_type=BackoffType.EXPONENTIAL,
        initial_delay=1.0,
    )
)
def flaky_api_call(endpoint: str):
    """Simulates a flaky API that fails 70% of the time."""
    print(f"Calling API: {endpoint}")
    
    if random.random() < 0.7:
        raise Exception(f"API call failed: Connection timeout")
    
    return {"endpoint": endpoint, "status": "success"}


# Fibonacci backoff
@task(
    queue="critical",
    retry=RetryPolicy(
        max_retries=5,
        backoff_type=BackoffType.FIBONACCI,
        initial_delay=0.5,
        max_delay=30.0,
    )
)
def critical_operation(data: dict):
    """A critical operation that uses fibonacci backoff."""
    print(f"Processing critical data: {data}")
    
    if random.random() < 0.5:
        raise Exception("Critical operation failed: Database locked")
    
    return {"processed": True, "data": data}


# Linear backoff
@task(
    queue="batch",
    retry=RetryPolicy(
        max_retries=4,
        backoff_type=BackoffType.LINEAR,
        initial_delay=2.0,
    )
)
def batch_process(items: list):
    """Batch processing with linear backoff."""
    print(f"Processing batch of {len(items)} items...")
    
    if random.random() < 0.3:
        raise Exception("Batch processing failed: Out of memory")
    
    return {"processed_count": len(items)}


# Constant backoff (no increase)
@task(
    queue="polling",
    retry=RetryPolicy(
        max_retries=10,
        backoff_type=BackoffType.CONSTANT,
        initial_delay=5.0,
        jitter=False,
    )
)
def poll_external_service(service_url: str):
    """Poll an external service with constant retry delay."""
    print(f"Polling: {service_url}")
    
    if random.random() < 0.8:
        raise Exception("Service not ready")
    
    return {"service": service_url, "status": "ready"}


def demonstrate_retry_policies():
    print("=" * 60)
    print("TideQueue Retry Policy Demonstration")
    print("=" * 60)
    
    from tidequeue.retry import RetryEngine
    from tidequeue.models import Job
    
    engine = RetryEngine()
    
    policies = [
        ("Exponential", RetryPolicy(max_retries=5, backoff_type=BackoffType.EXPONENTIAL, initial_delay=1.0)),
        ("Linear", RetryPolicy(max_retries=5, backoff_type=BackoffType.LINEAR, initial_delay=1.0)),
        ("Fibonacci", RetryPolicy(max_retries=5, backoff_type=BackoffType.FIBONACCI, initial_delay=1.0)),
        ("Constant", RetryPolicy(max_retries=5, backoff_type=BackoffType.CONSTANT, initial_delay=2.0)),
    ]
    
    print("\nRetry delay progression for each backoff type:")
    print("-" * 60)
    
    for name, policy in policies:
        job = Job.create(name="test_task", retry_policy=policy)
        
        delays = []
        for attempt in range(1, 6):
            job.retry_count = attempt
            delay = engine.get_next_delay(job)
            delays.append(f"{delay:.1f}s")
        
        print(f"{name:12} backoff: {' -> '.join(delays)}")
    
    print("\n")


def main():
    demonstrate_retry_policies()
    
    print("Starting live retry example...")
    print("-" * 60)
    
    # Seed random for reproducible results
    random.seed(42)
    
    config = Config(db_path="./retry_example.db", worker_count=2)
    tq = TideQueue(config)
    
    # Submit jobs
    job_ids = []
    
    job_ids.append(tq.enqueue("flaky_api_call", args=["https://api.example.com/data"]))
    job_ids.append(tq.enqueue("critical_operation", kwargs={"data": {"id": 123, "value": "test"}}))
    job_ids.append(tq.enqueue("batch_process", args=[[1, 2, 3, 4, 5]]))
    
    print(f"\nSubmitted {len(job_ids)} jobs with various retry policies")
    
    # Start workers
    pool = WorkerPool(
        tq.broker, 
        size=2, 
        queues=["default", "critical", "batch", "polling"]
    )
    pool.start()
    
    print("Workers started. Processing jobs with retries...\n")
    
    # Wait and show progress
    for i in range(10):
        time.sleep(1)
        
        stats = tq.broker.get_stats()
        print(f"[{i+1}s] Pending: {stats['pending']}, Running: {stats['running']}, "
              f"Completed: {stats['completed']}, Failed: {stats['failed']}")
        
        if stats['pending'] == 0 and stats['running'] == 0:
            break
    
    print("\n" + "-" * 60)
    print("Final job statuses:")
    
    for job_id in job_ids:
        job = tq.get_job(job_id)
        status = f"{job.status.value}"
        retries = f"(retries: {job.retry_count}/{job.retry_policy.max_retries})"
        error = f" - Error: {job.error}" if job.error else ""
        print(f"  {job.name}: {status} {retries}{error}")
    
    pool.stop()
    
    # Cleanup
    if os.path.exists("./retry_example.db"):
        os.remove("./retry_example.db")
    
    print("\n" + "=" * 60)
    print("Retry example complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
