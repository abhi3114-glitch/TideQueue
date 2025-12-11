"""
TideQueue Configuration Management
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Config:
    """Configuration for TideQueue."""
    
    # Database settings
    db_path: str = field(default_factory=lambda: os.getenv("TIDEQUEUE_DB_PATH", "./tidequeue.db"))
    
    # Worker settings
    worker_count: int = field(default_factory=lambda: int(os.getenv("TIDEQUEUE_WORKERS", "4")))
    worker_timeout: int = field(default_factory=lambda: int(os.getenv("TIDEQUEUE_TIMEOUT", "300")))
    heartbeat_interval: int = 5  # seconds
    
    # Queue settings
    default_queue: str = "default"
    max_queue_size: int = field(default_factory=lambda: int(os.getenv("TIDEQUEUE_MAX_QUEUE_SIZE", "10000")))
    
    # Retry settings
    default_max_retries: int = 3
    default_retry_delay: float = 1.0  # seconds
    max_retry_delay: float = 3600.0  # 1 hour max
    
    # Logging
    log_level: str = field(default_factory=lambda: os.getenv("TIDEQUEUE_LOG_LEVEL", "INFO"))
    
    # API settings
    api_host: str = field(default_factory=lambda: os.getenv("TIDEQUEUE_API_HOST", "0.0.0.0"))
    api_port: int = field(default_factory=lambda: int(os.getenv("TIDEQUEUE_API_PORT", "8000")))
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        if self.max_queue_size < 1:
            raise ValueError("max_queue_size must be at least 1")
        if self.default_max_retries < 0:
            raise ValueError("default_max_retries must be non-negative")
