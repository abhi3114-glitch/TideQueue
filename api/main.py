"""
TideQueue REST API Main Application
"""

import os
import sys
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tidequeue.config import Config
from tidequeue.broker import Broker
from tidequeue.pool import WorkerPool
from tidequeue.dlq import DeadLetterQueue

from .routes import jobs, workers, queues, dlq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global instances
_config: Config = None
_broker: Broker = None
_pool: WorkerPool = None
_dlq: DeadLetterQueue = None


def get_app_broker() -> Broker:
    return _broker


def get_app_pool() -> WorkerPool:
    return _pool


def get_app_dlq() -> DeadLetterQueue:
    return _dlq


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global _config, _broker, _pool, _dlq
    
    logger.info("Starting TideQueue API...")
    
    # Initialize components
    _config = Config()
    _broker = Broker(_config)
    _dlq = DeadLetterQueue(_broker.persistence)
    
    # Start worker pool
    _pool = WorkerPool(_broker, size=_config.worker_count)
    _pool.start()
    
    logger.info(f"TideQueue API started with {_config.worker_count} workers")
    
    yield
    
    # Shutdown
    logger.info("Shutting down TideQueue API...")
    
    if _pool:
        _pool.stop()
    
    if _broker:
        _broker.close()
    
    logger.info("TideQueue API shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="TideQueue Admin API",
    description="REST API for managing TideQueue distributed task queue",
    version="1.0.0",
    lifespan=lifespan,
)

# Register routes
app.include_router(jobs.router, prefix="/api")
app.include_router(workers.router, prefix="/api")
app.include_router(queues.router, prefix="/api")
app.include_router(dlq.router, prefix="/api")


class HealthResponse(BaseModel):
    status: str
    version: str


class StatsResponse(BaseModel):
    queues: int
    pending: int
    running: int
    completed: int
    failed: int
    dead: int
    total: int
    workers: Dict[str, Any]


@app.get("/api/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Health check endpoint."""
    return HealthResponse(status="ok", version="1.0.0")


@app.get("/api/stats", response_model=StatsResponse, tags=["System"])
async def get_stats():
    """Get overall system statistics."""
    broker_stats = _broker.get_stats()
    pool_stats = _pool.get_stats() if _pool else {}
    
    return StatsResponse(
        queues=broker_stats["queues"],
        pending=broker_stats["pending"],
        running=broker_stats["running"],
        completed=broker_stats["completed"],
        failed=broker_stats["failed"],
        dead=broker_stats["dead"],
        total=broker_stats["total"],
        workers=pool_stats,
    )


# Serve static files for dashboard
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/", tags=["Dashboard"])
async def serve_dashboard():
    """Serve the admin dashboard."""
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Dashboard not found. Please check static files."}


if __name__ == "__main__":
    import uvicorn
    
    config = Config()
    uvicorn.run(
        "api.main:app",
        host=config.api_host,
        port=config.api_port,
        reload=True,
    )
