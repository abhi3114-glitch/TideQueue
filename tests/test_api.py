"""
Tests for TideQueue REST API
"""

import pytest
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient


@pytest.fixture
def temp_db():
    """Create a temporary database file."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def client(temp_db, monkeypatch):
    """Create a test client with temporary database."""
    # Set env var before importing app
    monkeypatch.setenv("TIDEQUEUE_DB_PATH", temp_db)
    monkeypatch.setenv("TIDEQUEUE_WORKERS", "1")
    
    from api.main import app
    
    with TestClient(app) as client:
        yield client


class TestHealthEndpoint:
    """Tests for health check endpoint."""
    
    def test_health_check(self, client):
        """Test health check returns OK."""
        response = client.get("/api/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "version" in data


class TestStatsEndpoint:
    """Tests for stats endpoint."""
    
    def test_get_stats(self, client):
        """Test getting system stats."""
        response = client.get("/api/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "pending" in data
        assert "completed" in data
        assert "failed" in data
        assert "workers" in data


class TestJobsAPI:
    """Tests for jobs API endpoints."""
    
    def test_create_job(self, client):
        """Test creating a new job."""
        response = client.post("/api/jobs", json={
            "task": "test_task",
            "args": [1, 2],
            "kwargs": {"key": "value"},
            "queue": "default",
            "priority": 5,
        })
        
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert data["status"] == "pending"
    
    def test_list_jobs(self, client):
        """Test listing jobs."""
        # Create some jobs first
        for i in range(3):
            client.post("/api/jobs", json={"task": f"task_{i}"})
        
        response = client.get("/api/jobs")
        
        assert response.status_code == 200
        data = response.json()
        assert "jobs" in data
        assert len(data["jobs"]) >= 3
    
    def test_list_jobs_with_status_filter(self, client):
        """Test listing jobs with status filter."""
        client.post("/api/jobs", json={"task": "test_task"})
        
        response = client.get("/api/jobs?status=pending")
        
        assert response.status_code == 200
        data = response.json()
        assert all(job["status"] == "pending" for job in data["jobs"])
    
    def test_get_job(self, client):
        """Test getting a specific job."""
        create_response = client.post("/api/jobs", json={"task": "test_task"})
        job_id = create_response.json()["id"]
        
        response = client.get(f"/api/jobs/{job_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == job_id
        assert data["name"] == "test_task"
    
    def test_get_nonexistent_job(self, client):
        """Test getting a non-existent job."""
        response = client.get("/api/jobs/nonexistent-id")
        
        assert response.status_code == 404
    
    def test_cancel_job(self, client):
        """Test cancelling a pending job."""
        create_response = client.post("/api/jobs", json={"task": "test_task"})
        job_id = create_response.json()["id"]
        
        response = client.delete(f"/api/jobs/{job_id}")
        
        assert response.status_code == 200
        
        # Verify job is cancelled
        get_response = client.get(f"/api/jobs/{job_id}")
        assert get_response.json()["status"] == "cancelled"


class TestWorkersAPI:
    """Tests for workers API endpoints."""
    
    def test_list_workers(self, client):
        """Test listing workers."""
        response = client.get("/api/workers")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
    
    def test_get_worker_stats(self, client):
        """Test getting worker stats."""
        response = client.get("/api/workers/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "total_workers" in data
        assert "idle" in data
        assert "busy" in data
    
    def test_scale_workers(self, client):
        """Test scaling worker pool."""
        response = client.post("/api/workers/scale", json={"size": 2})
        
        assert response.status_code == 200
        data = response.json()
        assert data["new_size"] == 2
    
    def test_scale_workers_invalid_size(self, client):
        """Test scaling with invalid size."""
        response = client.post("/api/workers/scale", json={"size": 0})
        
        assert response.status_code == 400


class TestQueuesAPI:
    """Tests for queues API endpoints."""
    
    def test_list_queues(self, client):
        """Test listing queues."""
        # Create a job to ensure at least one queue exists
        client.post("/api/jobs", json={"task": "test_task", "queue": "test_queue"})
        
        response = client.get("/api/queues")
        
        assert response.status_code == 200
        data = response.json()
        assert "queues" in data
    
    def test_get_queue(self, client):
        """Test getting a specific queue."""
        response = client.get("/api/queues/default")
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "default"
        assert "pending" in data
        assert "completed" in data


class TestDLQAPI:
    """Tests for dead-letter queue API endpoints."""
    
    def test_list_dlq(self, client):
        """Test listing DLQ jobs."""
        response = client.get("/api/dlq")
        
        assert response.status_code == 200
        data = response.json()
        assert "jobs" in data
        assert "total" in data
    
    def test_get_dlq_stats(self, client):
        """Test getting DLQ stats."""
        response = client.get("/api/dlq/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "by_task" in data
        assert "by_error" in data
