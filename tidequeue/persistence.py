"""
TideQueue Persistence Layer - SQLite-backed job storage
"""

import sqlite3
import json
import threading
from datetime import datetime
from typing import List, Optional, Dict, Any
from contextlib import contextmanager

from .models import Job, JobStatus, QueueStats
from .config import Config


class PersistenceLayer:
    """SQLite-backed persistence for jobs and queue state."""
    
    def __init__(self, config: Config):
        self.config = config
        self.db_path = config.db_path
        self._local = threading.local()
        self._init_db()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn
    
    @contextmanager
    def _cursor(self):
        """Context manager for database cursor."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    
    def _init_db(self):
        """Initialize database schema."""
        with self._cursor() as cursor:
            # Jobs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    args TEXT NOT NULL,
                    kwargs TEXT NOT NULL,
                    queue TEXT NOT NULL DEFAULT 'default',
                    priority INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    retry_policy TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    traceback TEXT,
                    created_at TEXT NOT NULL,
                    scheduled_at TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    worker_id TEXT
                )
            """)
            
            # Create indexes for common queries
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(queue)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at)")
            
            # Dead letter queue table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dlq_jobs (
                    id TEXT PRIMARY KEY,
                    job_data TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    moved_at TEXT NOT NULL
                )
            """)
            
            # Worker heartbeats table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS worker_heartbeats (
                    worker_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    current_job_id TEXT,
                    jobs_completed INTEGER NOT NULL DEFAULT 0,
                    jobs_failed INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT NOT NULL,
                    last_heartbeat TEXT NOT NULL
                )
            """)
    
    def save_job(self, job: Job) -> None:
        """Save or update a job."""
        with self._cursor() as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO jobs 
                (id, name, args, kwargs, queue, priority, status, retry_count,
                 retry_policy, result, error, traceback, created_at, scheduled_at,
                 started_at, completed_at, worker_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job.id,
                job.name,
                json.dumps(job.args),
                json.dumps(job.kwargs),
                job.queue,
                job.priority,
                job.status.value,
                job.retry_count,
                json.dumps(job.retry_policy.to_dict()),
                json.dumps(job.result) if job.result is not None else None,
                job.error,
                job.traceback,
                job.created_at.isoformat() if job.created_at else None,
                job.scheduled_at.isoformat() if job.scheduled_at else None,
                job.started_at.isoformat() if job.started_at else None,
                job.completed_at.isoformat() if job.completed_at else None,
                job.worker_id,
            ))
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID."""
        with self._cursor() as cursor:
            cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            if row:
                return self._row_to_job(row)
        return None
    
    def delete_job(self, job_id: str) -> bool:
        """Delete a job by ID."""
        with self._cursor() as cursor:
            cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            return cursor.rowcount > 0
    
    def get_pending_jobs(self, queue: str = None, limit: int = 100) -> List[Job]:
        """Get pending jobs ordered by priority and creation time."""
        with self._cursor() as cursor:
            if queue:
                cursor.execute("""
                    SELECT * FROM jobs 
                    WHERE status = 'pending' AND queue = ?
                    ORDER BY priority DESC, created_at ASC
                    LIMIT ?
                """, (queue, limit))
            else:
                cursor.execute("""
                    SELECT * FROM jobs 
                    WHERE status = 'pending'
                    ORDER BY priority DESC, created_at ASC
                    LIMIT ?
                """, (limit,))
            return [self._row_to_job(row) for row in cursor.fetchall()]
    
    def get_scheduled_jobs(self, before: datetime = None) -> List[Job]:
        """Get scheduled jobs that are ready to run."""
        before = before or datetime.utcnow()
        with self._cursor() as cursor:
            cursor.execute("""
                SELECT * FROM jobs 
                WHERE status = 'scheduled' AND scheduled_at <= ?
                ORDER BY priority DESC, scheduled_at ASC
            """, (before.isoformat(),))
            return [self._row_to_job(row) for row in cursor.fetchall()]
    
    def get_jobs_by_status(self, status: JobStatus, limit: int = 100) -> List[Job]:
        """Get jobs by status."""
        with self._cursor() as cursor:
            cursor.execute("""
                SELECT * FROM jobs WHERE status = ?
                ORDER BY created_at DESC LIMIT ?
            """, (status.value, limit))
            return [self._row_to_job(row) for row in cursor.fetchall()]
    
    def get_all_jobs(self, limit: int = 100, offset: int = 0) -> List[Job]:
        """Get all jobs with pagination."""
        with self._cursor() as cursor:
            cursor.execute("""
                SELECT * FROM jobs 
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            """, (limit, offset))
            return [self._row_to_job(row) for row in cursor.fetchall()]
    
    def get_queue_stats(self, queue: str = None) -> Dict[str, QueueStats]:
        """Get statistics for queues."""
        stats = {}
        with self._cursor() as cursor:
            if queue:
                cursor.execute("""
                    SELECT queue, status, COUNT(*) as count
                    FROM jobs WHERE queue = ?
                    GROUP BY queue, status
                """, (queue,))
            else:
                cursor.execute("""
                    SELECT queue, status, COUNT(*) as count
                    FROM jobs
                    GROUP BY queue, status
                """)
            
            for row in cursor.fetchall():
                queue_name = row['queue']
                if queue_name not in stats:
                    stats[queue_name] = QueueStats(name=queue_name)
                
                status = row['status']
                count = row['count']
                
                if status == 'pending':
                    stats[queue_name].pending = count
                elif status == 'running':
                    stats[queue_name].running = count
                elif status == 'completed':
                    stats[queue_name].completed = count
                elif status == 'failed':
                    stats[queue_name].failed = count
                elif status == 'dead':
                    stats[queue_name].dead = count
        
        return stats
    
    def move_to_dlq(self, job: Job, reason: str) -> None:
        """Move a job to the dead-letter queue."""
        with self._cursor() as cursor:
            # Insert into DLQ
            cursor.execute("""
                INSERT INTO dlq_jobs (id, job_data, reason, moved_at)
                VALUES (?, ?, ?, ?)
            """, (job.id, job.to_json(), reason, datetime.utcnow().isoformat()))
            
            # Update job status
            cursor.execute("""
                UPDATE jobs SET status = 'dead' WHERE id = ?
            """, (job.id,))
    
    def get_dlq_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get jobs in the dead-letter queue."""
        with self._cursor() as cursor:
            cursor.execute("""
                SELECT * FROM dlq_jobs ORDER BY moved_at DESC LIMIT ?
            """, (limit,))
            return [
                {
                    "id": row['id'],
                    "job": Job.from_json(row['job_data']),
                    "reason": row['reason'],
                    "moved_at": row['moved_at'],
                }
                for row in cursor.fetchall()
            ]
    
    def remove_from_dlq(self, job_id: str) -> Optional[Job]:
        """Remove a job from the DLQ and return it."""
        with self._cursor() as cursor:
            cursor.execute("SELECT job_data FROM dlq_jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            if row:
                job = Job.from_json(row['job_data'])
                cursor.execute("DELETE FROM dlq_jobs WHERE id = ?", (job_id,))
                return job
        return None
    
    def update_worker_heartbeat(self, worker_id: str, status: str, 
                                  current_job_id: str = None,
                                  jobs_completed: int = 0,
                                  jobs_failed: int = 0) -> None:
        """Update worker heartbeat."""
        now = datetime.utcnow().isoformat()
        with self._cursor() as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO worker_heartbeats 
                (worker_id, status, current_job_id, jobs_completed, jobs_failed, started_at, last_heartbeat)
                VALUES (?, ?, ?, ?, ?, COALESCE(
                    (SELECT started_at FROM worker_heartbeats WHERE worker_id = ?), ?
                ), ?)
            """, (worker_id, status, current_job_id, jobs_completed, jobs_failed, 
                  worker_id, now, now))
    
    def get_worker_heartbeats(self) -> List[Dict[str, Any]]:
        """Get all worker heartbeats."""
        with self._cursor() as cursor:
            cursor.execute("SELECT * FROM worker_heartbeats ORDER BY worker_id")
            return [dict(row) for row in cursor.fetchall()]
    
    def remove_worker(self, worker_id: str) -> None:
        """Remove a worker from heartbeats."""
        with self._cursor() as cursor:
            cursor.execute("DELETE FROM worker_heartbeats WHERE worker_id = ?", (worker_id,))
    
    def _row_to_job(self, row: sqlite3.Row) -> Job:
        """Convert a database row to a Job object."""
        from .models import RetryPolicy
        
        return Job(
            id=row['id'],
            name=row['name'],
            args=json.loads(row['args']),
            kwargs=json.loads(row['kwargs']),
            queue=row['queue'],
            priority=row['priority'],
            status=JobStatus(row['status']),
            retry_count=row['retry_count'],
            retry_policy=RetryPolicy.from_dict(json.loads(row['retry_policy'])),
            result=json.loads(row['result']) if row['result'] else None,
            error=row['error'],
            traceback=row['traceback'],
            created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
            scheduled_at=datetime.fromisoformat(row['scheduled_at']) if row['scheduled_at'] else None,
            started_at=datetime.fromisoformat(row['started_at']) if row['started_at'] else None,
            completed_at=datetime.fromisoformat(row['completed_at']) if row['completed_at'] else None,
            worker_id=row['worker_id'],
        )
    
    def close(self):
        """Close database connection."""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
