# TideQueue

A lightweight, production-ready distributed background-worker engine inspired by Celery and RabbitMQ. Built entirely in Python with SQLite persistence.

## Features

- **Job Queue**: Priority-aware task queuing with persistent storage
- **Retry Policy**: Configurable retry strategies with exponential, linear, fibonacci, and constant backoff
- **Worker Pool**: Multi-threaded worker management with dynamic scaling
- **Dead-Letter Queue**: Automatic handling of permanently failed jobs with replay capability
- **REST Admin Dashboard**: Full visibility and control over the system via web UI

## Architecture

```
+-------------------+       +------------------+       +------------------+
|    Producers      |       |   TideQueue      |       |   Worker Pool    |
| (API/Scheduler)   | ----> |   Broker         | ----> |   (N Workers)    |
+-------------------+       +------------------+       +------------------+
                                    |                          |
                                    v                          v
                            +------------------+       +------------------+
                            |   SQLite         |       |   Retry Engine   |
                            |   Persistence    |       +------------------+
                            +------------------+               |
                                                               v
                                                       +------------------+
                                                       |   Dead-Letter    |
                                                       |   Queue          |
                                                       +------------------+
```

## Installation

```bash
git clone https://github.com/abhi3114-glitch/TideQueue.git
cd TideQueue
pip install -r requirements.txt
```

## Quick Start

### 1. Define Tasks

```python
from tidequeue import task, RetryPolicy

@task(queue='default', retry=RetryPolicy(max_retries=3))
def send_email(to, subject, body):
    print(f"Sending email to {to}")
    return True
```

### 2. Submit Jobs

```python
from tidequeue import TideQueue

tq = TideQueue()
job_id = tq.enqueue('send_email', args=['user@example.com', 'Hello', 'World'])
print(f"Job submitted: {job_id}")
```

### 3. Start Workers

```python
from tidequeue import WorkerPool

pool = WorkerPool(size=4)
pool.start()
```

### 4. Run Admin Dashboard

```bash
python -m api.main
```

Open http://localhost:8000 in your browser to access the admin dashboard.

## REST API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/stats` | Overall statistics |
| POST | `/api/jobs` | Submit new job |
| GET | `/api/jobs` | List jobs with optional filters |
| GET | `/api/jobs/{id}` | Get job details |
| DELETE | `/api/jobs/{id}` | Cancel pending job |
| POST | `/api/jobs/{id}/retry` | Retry failed job |
| GET | `/api/queues` | List queues and stats |
| GET | `/api/workers` | List workers and status |
| POST | `/api/workers/scale` | Scale worker pool |
| GET | `/api/dlq` | List dead-letter jobs |
| POST | `/api/dlq/{id}/replay` | Replay job from DLQ |
| DELETE | `/api/dlq/{id}` | Purge job from DLQ |

## Retry Policies

TideQueue supports four backoff strategies:

| Strategy | Description | Example Delays |
|----------|-------------|----------------|
| Constant | Same delay every retry | 5s, 5s, 5s, 5s |
| Linear | Delay increases linearly | 2s, 4s, 6s, 8s |
| Exponential | Delay doubles each retry | 1s, 2s, 4s, 8s |
| Fibonacci | Delay follows fibonacci sequence | 1s, 1s, 2s, 3s, 5s |

```python
from tidequeue import RetryPolicy
from tidequeue.models import BackoffType

policy = RetryPolicy(
    max_retries=5,
    backoff_type=BackoffType.EXPONENTIAL,
    initial_delay=1.0,
    max_delay=3600.0,
    jitter=True,  # Prevents thundering herd
)
```

## Configuration

Environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `TIDEQUEUE_DB_PATH` | SQLite database path | `./tidequeue.db` |
| `TIDEQUEUE_WORKERS` | Number of workers | `4` |
| `TIDEQUEUE_TIMEOUT` | Job execution timeout (seconds) | `300` |
| `TIDEQUEUE_LOG_LEVEL` | Logging level | `INFO` |
| `TIDEQUEUE_API_HOST` | API host | `0.0.0.0` |
| `TIDEQUEUE_API_PORT` | API port | `8000` |

## Project Structure

```
TideQueue/
├── tidequeue/              # Core package
│   ├── models.py           # Data models (Job, RetryPolicy, WorkerState)
│   ├── broker.py           # Message broker with priority queues
│   ├── persistence.py      # SQLite storage layer
│   ├── retry.py            # Retry engine with backoff strategies
│   ├── worker.py           # Individual worker implementation
│   ├── pool.py             # Worker pool management
│   ├── dlq.py              # Dead-letter queue handler
│   └── decorators.py       # @task decorator
├── api/                    # REST API
│   ├── main.py             # FastAPI application
│   ├── routes/             # API endpoint handlers
│   └── static/             # Dashboard HTML/CSS/JS
├── examples/               # Usage examples
│   ├── simple_task.py      # Basic usage
│   └── advanced_retry.py   # Retry policy demonstration
├── tests/                  # Test suite
├── requirements.txt        # Dependencies
└── pyproject.toml          # Project metadata
```

## Running Tests

```bash
python -m pytest tests/ -v
```

## Skills Demonstrated

- Queue data structures and priority scheduling
- Distributed messaging patterns
- Worker orchestration and process management
- Retry policies with backoff algorithms
- RESTful API design
- Real-time dashboard development
- SQLite persistence and recovery

## License

MIT License
