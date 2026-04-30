# Trilingual Gateway

A polyglot microservices platform built with **Python**, **Go**, and **TypeScript**. Each service handles a distinct domain — analytics event tracking, real-time message processing, and user management — and exposes a RESTful API with health checks and structured logging.

## Architecture

```mermaid
graph TB
    Client[Client / API Consumer]

    subgraph Docker Compose
        PY[Analytics Service<br/>Python / Flask<br/>:8001]
        GO[Processor Service<br/>Go / net/http<br/>:8002]
        TS[User Mgmt Service<br/>TypeScript / Express<br/>:8003]
    end

    Client -->|POST/GET /api/events| PY
    Client -->|POST/GET /api/messages| GO
    Client -->|CRUD /api/users| TS

    PY --- HC1[/health]
    GO --- HC2[/health]
    TS --- HC3[/health]
```

## Services

| Service | Language | Port | Description |
|---------|----------|------|-------------|
| analytics-py | Python 3.12 (Flask) | 8001 | Event tracking and analytics aggregation |
| processor-go | Go 1.22 (net/http) | 8002 | Real-time message processing with channel-based routing |
| usermgmt-ts | TypeScript (Express) | 8003 | User CRUD operations with email uniqueness enforcement |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- (For local dev) Python 3.12+, Go 1.22+, Node.js 22+

### Run with Docker Compose

```bash
cp .env.example .env
make up        # Build and start all services
make ps        # Check service status
make logs      # Tail logs
make down      # Stop all services
```

### Run Tests Locally

```bash
make test          # Run all tests
make test-python   # Python tests only
make test-go       # Go tests only
make test-ts       # TypeScript tests only
make lint          # Run all linters
```

## API Reference

### Analytics Service (`:8001`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/api/events` | Track an event |
| GET | `/api/events` | List events (`?event_name=`, `?limit=`, `?offset=`) |
| DELETE | `/api/events` | Delete events by name (`?event_name=` required) |
| GET | `/api/events/summary` | Aggregated event counts by name |

**Example:**
```bash
# Track an event
curl -X POST http://localhost:8001/api/events \
  -H "Content-Type: application/json" \
  -d '{"event_name": "page_view", "properties": {"page": "/home"}}'

# List with pagination
curl "http://localhost:8001/api/events?limit=10&offset=0"

# Get summary
curl http://localhost:8001/api/events/summary
```

### Processor Service (`:8002`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/api/messages` | Publish a message to a channel |
| GET | `/api/messages` | List messages (optional `?channel=` filter) |
| GET | `/api/stats` | Message count per channel |

**Example:**
```bash
# Publish a message
curl -X POST http://localhost:8002/api/messages \
  -H "Content-Type: application/json" \
  -d '{"channel": "alerts", "payload": "CPU usage high"}'

# Get stats
curl http://localhost:8002/api/stats
```

### User Management Service (`:8003`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/api/users` | Create a user |
| GET | `/api/users` | List all users |
| GET | `/api/users/:id` | Get user by ID |
| PUT | `/api/users/:id` | Update a user (partial update) |
| DELETE | `/api/users/:id` | Delete a user |

**Example:**
```bash
# Create a user
curl -X POST http://localhost:8003/api/users \
  -H "Content-Type: application/json" \
  -d '{"username": "alice", "email": "alice@example.com", "role": "admin"}'

# List users
curl http://localhost:8003/api/users
```

## Environment Variables

See [`.env.example`](.env.example) for all available configuration:

| Variable | Default | Description |
|----------|---------|-------------|
| `ANALYTICS_PORT` | 8001 | Analytics service port |
| `LOG_LEVEL` | INFO | Python logging level |
| `MAX_EVENTS` | 10000 | Maximum events stored in memory (oldest evicted) |
| `DEFAULT_PAGE_LIMIT` | 50 | Default page size for event listing |
| `PROCESSOR_PORT` | 8002 | Processor service port |
| `USERMGMT_PORT` | 8003 | User management service port |

## CI/CD

GitHub Actions workflow runs on every push and PR to `main`:

1. **test-python** — Lint with flake8, test with pytest
2. **test-go** — Vet and test Go code
3. **test-typescript** — Lint with ESLint, test with Jest
4. **docker-build** — Verify all Dockerfiles build successfully

> **Note:** The `.github/workflows/ci.yml` file may need to be manually added after initial repository setup due to GitHub API limitations.

## Project Structure

```
trilingual-gateway/
├── docker-compose.yml
├── Makefile
├── .env.example
├── .gitignore
├── README.md
├── .github/
│   └── workflows/
│       └── ci.yml
└── services/
    ├── analytics-py/          # Python analytics service
    │   ├── Dockerfile
    │   ├── app.py
    │   ├── requirements.txt
    │   └── test_app.py
    ├── processor-go/          # Go message processor
    │   ├── Dockerfile
    │   ├── go.mod
    │   ├── main.go
    │   └── main_test.go
    └── usermgmt-ts/           # TypeScript user management
        ├── Dockerfile
        ├── package.json
        ├── tsconfig.json
        ├── jest.config.js
        ├── .eslintrc.json
        └── src/
            ├── app.ts
            └── app.test.ts
```

## License

MIT
