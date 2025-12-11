"""
TideQueue Task Decorators
"""

import functools
from typing import Callable, Optional, Any

from .models import RetryPolicy, Job


# Global task registry
_task_registry = {}


def get_task_registry():
    """Get the global task registry."""
    return _task_registry


def get_task(name: str) -> Optional[Callable]:
    """Get a registered task by name."""
    return _task_registry.get(name)


def task(
    name: str = None,
    queue: str = "default",
    retry: RetryPolicy = None,
    timeout: int = None,
):
    """
    Decorator to register a function as a TideQueue task.
    
    Usage:
        @task(queue='emails', retry=RetryPolicy(max_retries=5))
        def send_email(to, subject, body):
            # Task implementation
            pass
        
        # Submit the task
        send_email.delay('user@example.com', 'Hello', 'World')
    
    Args:
        name: Task name (defaults to function name)
        queue: Queue to submit to
        retry: Retry policy for this task
        timeout: Execution timeout in seconds
    """
    def decorator(func: Callable) -> Callable:
        task_name = name or func.__name__
        task_queue = queue
        task_retry = retry or RetryPolicy()
        task_timeout = timeout
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Direct execution
            return func(*args, **kwargs)
        
        # Add task metadata
        wrapper._is_task = True
        wrapper._task_name = task_name
        wrapper._task_queue = task_queue
        wrapper._task_retry = task_retry
        wrapper._task_timeout = task_timeout
        wrapper._original_func = func
        
        def delay(*args, **kwargs) -> str:
            """Submit this task to the queue."""
            from . import TideQueue
            
            tq = TideQueue()
            job = Job.create(
                name=task_name,
                args=list(args),
                kwargs=kwargs,
                queue=task_queue,
                retry_policy=task_retry,
            )
            return tq.enqueue(
                task_name,
                args=list(args),
                kwargs=kwargs,
                queue=task_queue,
            )
        
        def apply_async(
            args: list = None,
            kwargs: dict = None,
            queue: str = None,
            priority: int = 0,
        ) -> str:
            """Submit this task with custom options."""
            from . import TideQueue
            
            tq = TideQueue()
            return tq.enqueue(
                task_name,
                args=args or [],
                kwargs=kwargs or {},
                queue=queue or task_queue,
                priority=priority,
            )
        
        # Attach helper methods
        wrapper.delay = delay
        wrapper.apply_async = apply_async
        
        # Register the task
        _task_registry[task_name] = func
        
        return wrapper
    
    return decorator


class TaskExecutor:
    """
    Executes registered tasks.
    """
    
    def __init__(self):
        self.registry = _task_registry
    
    def execute(self, job: Job) -> Any:
        """
        Execute a job's task.
        
        Returns the task result.
        Raises exception if task fails.
        """
        task_func = self.registry.get(job.name)
        
        if task_func is None:
            from .exceptions import TaskNotRegisteredError
            raise TaskNotRegisteredError(job.name)
        
        # Execute the task
        return task_func(*job.args, **job.kwargs)
    
    def is_registered(self, task_name: str) -> bool:
        """Check if a task is registered."""
        return task_name in self.registry
    
    def list_tasks(self) -> list:
        """List all registered tasks."""
        return list(self.registry.keys())
