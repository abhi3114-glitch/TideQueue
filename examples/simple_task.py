"""
TideQueue Simple Task Example

This example shows basic usage of TideQueue for background job processing.
"""

import time
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tidequeue import TideQueue, task, RetryPolicy, WorkerPool
from tidequeue.config import Config


# Define a simple task
@task(queue="default")
def hello_world(name: str):
    """A simple greeting task."""
    print(f"Hello, {name}!")
    return f"Greeted {name}"


@task(queue="default")
def add_numbers(a: int, b: int):
    """Add two numbers."""
    result = a + b
    print(f"Adding {a} + {b} = {result}")
    return result


@task(queue="emails", retry=RetryPolicy(max_retries=3))
def send_email(to: str, subject: str, body: str):
    """Simulate sending an email."""
    print(f"Sending email to {to}...")
    print(f"  Subject: {subject}")
    print(f"  Body: {body}")
    time.sleep(0.5)  # Simulate network delay
    print(f"Email sent to {to}!")
    return {"to": to, "status": "sent"}


@task(queue="default")
def failing_task():
    """A task that always fails (for testing retries)."""
    raise Exception("This task always fails!")


def main():
    print("=" * 50)
    print("TideQueue Simple Example")
    print("=" * 50)
    
    # Initialize TideQueue
    config = Config(db_path="./example.db", worker_count=2)
    tq = TideQueue(config)
    
    print("\n1. Submitting jobs to the queue...")
    
    # Submit some jobs
    job1_id = tq.enqueue("hello_world", args=["Alice"])
    print(f"   Submitted job: {job1_id}")
    
    job2_id = tq.enqueue("add_numbers", args=[10, 20])
    print(f"   Submitted job: {job2_id}")
    
    job3_id = tq.enqueue("send_email", 
                          args=["user@example.com", "Welcome!", "Hello from TideQueue"],
                          queue="emails")
    print(f"   Submitted job: {job3_id}")
    
    print("\n2. Starting workers...")
    
    # Start workers
    pool = WorkerPool(tq.broker, size=2, queues=["default", "emails"])
    pool.start()
    
    print(f"   Started {pool.worker_count} workers")
    
    print("\n3. Waiting for jobs to complete...")
    time.sleep(3)
    
    print("\n4. Checking job status...")
    
    for job_id in [job1_id, job2_id, job3_id]:
        job = tq.get_job(job_id)
        print(f"   Job {job_id[:8]}... - Status: {job.status.value}, Result: {job.result}")
    
    print("\n5. Queue statistics:")
    stats = tq.broker.get_stats()
    print(f"   Pending: {stats['pending']}")
    print(f"   Running: {stats['running']}")
    print(f"   Completed: {stats['completed']}")
    print(f"   Failed: {stats['failed']}")
    
    print("\n6. Stopping workers...")
    pool.stop()
    
    print("\n" + "=" * 50)
    print("Example complete!")
    print("=" * 50)
    
    # Cleanup
    if os.path.exists("./example.db"):
        os.remove("./example.db")


if __name__ == "__main__":
    main()
