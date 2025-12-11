"""
TideQueue Exceptions
"""


class TideQueueError(Exception):
    """Base exception for TideQueue."""
    pass


class JobNotFoundError(TideQueueError):
    """Raised when a job is not found."""
    
    def __init__(self, job_id: str):
        self.job_id = job_id
        super().__init__(f"Job not found: {job_id}")


class QueueFullError(TideQueueError):
    """Raised when the queue is at capacity."""
    
    def __init__(self, queue_name: str, capacity: int):
        self.queue_name = queue_name
        self.capacity = capacity
        super().__init__(f"Queue '{queue_name}' is full (capacity: {capacity})")


class WorkerError(TideQueueError):
    """Raised when a worker encounters an error."""
    
    def __init__(self, worker_id: str, message: str):
        self.worker_id = worker_id
        super().__init__(f"Worker {worker_id}: {message}")


class RetryExhaustedError(TideQueueError):
    """Raised when all retry attempts are exhausted."""
    
    def __init__(self, job_id: str, max_retries: int):
        self.job_id = job_id
        self.max_retries = max_retries
        super().__init__(f"Job {job_id} exhausted all {max_retries} retry attempts")


class TaskNotRegisteredError(TideQueueError):
    """Raised when trying to execute an unregistered task."""
    
    def __init__(self, task_name: str):
        self.task_name = task_name
        super().__init__(f"Task not registered: {task_name}")


class SerializationError(TideQueueError):
    """Raised when job serialization/deserialization fails."""
    pass


class TimeoutError(TideQueueError):
    """Raised when a job execution times out."""
    
    def __init__(self, job_id: str, timeout: int):
        self.job_id = job_id
        self.timeout = timeout
        super().__init__(f"Job {job_id} timed out after {timeout} seconds")
