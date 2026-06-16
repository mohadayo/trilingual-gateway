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
| GET | `/api/events` | List events with filtering / pagination / sorting (see params below) |
| DELETE | `/api/events` | Delete events by name (`?event_name=` required) |
| GET | `/api/events/summary` | Aggregated event counts by name (filterable) |
| GET | `/api/events/names` | distinct な event_name のみを返す軽量エンドポイント（フィルタドロップダウン / オートコンプリート用） |
| GET | `/api/events/property_keys` | フィルタ後のイベントに登場した properties キー一覧（`event_name` / `q` / `since` / `until` / `order` / `limit` / `offset`） |
| GET | `/api/events/property_values/<key>` | 指定キーの distinct 値とその出現回数（`event_name` / `q` / `since` / `until` / `sort=value\|count` / `order` / `limit` / `offset`、既定は `count desc`） |

**`GET /api/events` query parameters:**
- `event_name`: 完全一致でイベント名を絞り込み
- `limit` / `offset`: ページネーション（`limit` 既定 `DEFAULT_PAGE_LIMIT`、上限 `MAX_PAGE_LIMIT`）
- `since` / `until`: ISO 8601 / RFC 3339 タイムスタンプで期間絞り込み（`since` ≤ `until`）
- `sort`: `timestamp`（既定）/ `event_name`
- `order`: `asc`（既定）/ `desc`

**`GET /api/events/summary` query parameters:** `event_name` / `since` / `until`（`/api/events` と同じ意味）

**`GET /api/events/names` query parameters:**
- `q`: event_name の大文字小文字無視部分一致
- `since` / `until`: ISO 8601 タイムスタンプ範囲フィルタ（`/api/events` と同じパース）
- `order`: `asc`（既定）/ `desc`（event_name 名昇順 / 降順）
- `limit` / `offset`: `DEFAULT_PAGE_LIMIT` / `MAX_PAGE_LIMIT` を流用

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

# Get distinct event_name list (light-weight)
curl http://localhost:8001/api/events/names
curl "http://localhost:8001/api/events/names?q=page&order=desc"
```

### Processor Service (`:8002`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/api/messages` | Publish a message to a channel |
| GET | `/api/messages` | List messages with filtering / pagination / sorting (see params below) |
| GET | `/api/messages/{id}` | Get a single message by ID（該当なしは `404`） |
| DELETE | `/api/messages` | `channel` / `since` / `before` の AND で一致するメッセージを削除（少なくとも 1 フィルタ必須。`since` は包含、`before` は排他で半開区間 `[since, before)` を表現） |
| DELETE | `/api/messages/{id}` | 単一メッセージを ID 指定で削除。レスポンスに削除前のメッセージ内容を含め、別 GET なしで監査ログに残せる。該当なしは `404` |
| GET | `/api/stats` | Message count per channel（`?channel=` / `?q=` / `?since=` / `?until=` でフィルタ後の集計を返す） |

**`GET /api/messages` query parameters:**
- `channel`: 完全一致でチャンネルを絞り込み
- `q`: `channel` / `payload` の大文字小文字無視の部分一致（最大 `MAX_SEARCH_LENGTH` 文字）
- `limit` / `offset`: ページネーション（`limit` 既定 `DEFAULT_PAGE_LIMIT`、上限 `MAX_PAGE_LIMIT`）
- `since` / `until`: ISO 8601 / RFC 3339 タイムスタンプで期間絞り込み（`until` ≥ `since`）
- `sort`: `created_at`（既定）/ `channel` / `id`
- `order`: `asc`（既定）/ `desc`

**`GET /api/stats` query parameters:** `channel` / `q` / `since` / `until` を `/api/messages` と同じセマンティクスで受け付け、フィルタ後のメッセージから集計値を返す。GET 以外のメソッドは 405。

レスポンス形：

```json
{
  "total_messages": 12,
  "channels": {"alerts": 9, "info": 3},
  "distinct_channels": 2,
  "oldest": "2030-01-01T00:00:00Z",
  "newest": "2030-12-31T23:59:59Z"
}
```

- `distinct_channels`: フィルタ通過後に登場した channel のユニーク数（`channels` マップのキー数と一致）
- `oldest` / `newest`: フィルタ通過後の `created_at` の最小・最大（RFC 3339）。マッチ 0 件のときは両方とも `null`。クライアントが追加クエリ無しに「いまフィルタ条件で残っているデータの時間範囲」を把握できる

集計は 1 スキャンで行うため、フィルタが付いても挙動コストは従来と変わらない。

**Validation rules (POST):**
- `channel`: 必須、トリム後 1〜`MAX_CHANNEL_LENGTH`（既定 256）文字
- `payload`: 必須、トリム後 1〜`MAX_PAYLOAD_LENGTH`（既定 65536）文字
- 空白のみの値や長さ超過は 400 を返す

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
| GET | `/api/users` | List users with filtering / search / pagination / sorting (see params below) |
| GET | `/api/users/:id` | Get user by ID |
| PUT | `/api/users/:id` | Update a user (partial update) |
| DELETE | `/api/users/:id` | Delete a user |

**`GET /api/users` query parameters:**
- `limit` / `offset`: ページネーション（`limit` 既定 `USERS_DEFAULT_LIMIT`、上限 `USERS_MAX_LIMIT`）
- `role`: `user` / `admin` / `moderator` で絞り込み
- `q`: `username` / `email` の部分一致検索（大文字小文字を無視）
- `sort`: `created_at`（既定）/ `updated_at` / `username` / `email` / `role`
- `order`: `asc`（既定）/ `desc`

**Validation rules (POST / PUT):**
- `username`: 必須（POSTのみ）、トリム後 1〜`MAX_USERNAME_LENGTH`（既定 50）文字
- `email`: 必須（POSTのみ）、トリム後 1〜254 文字、簡易メール形式チェック、**小文字に正規化**して保存（大文字違いの重複を防止）
- `role`: 任意、`user` / `admin` / `moderator` のいずれか（既定: `user`）
- 不正な値は 400 を返す

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
| `MAX_PAGE_LIMIT` | 500 | analytics-py: `GET /api/events` の `limit` 上限 |
| `MAX_PAYLOAD_SIZE` | 1048576 | analytics-py: リクエストボディの最大サイズ（バイト） |
| `MAX_EVENT_NAME_LENGTH` | 200 | analytics-py: `event_name` の最大文字数 |
| `MAX_CHANNEL_LENGTH` | 256 | processor-go: `POST /api/messages` の `channel` の最大文字数 |
| `MAX_PAYLOAD_LENGTH` | 65536 | processor-go: `POST /api/messages` の `payload` の最大文字数 |
| `PROCESSOR_PORT` | 8002 | Processor service port |
| `PROCESSOR_READ_HEADER_TIMEOUT` | 5 | processor-go: ヘッダ読み取りタイムアウト秒（Slowloris 対策） |
| `PROCESSOR_READ_TIMEOUT` | 15 | processor-go: 本文読み取りタイムアウト秒 |
| `PROCESSOR_WRITE_TIMEOUT` | 15 | processor-go: レスポンス書き込みタイムアウト秒 |
| `PROCESSOR_IDLE_TIMEOUT` | 60 | processor-go: Keep-Alive アイドルタイムアウト秒 |
| `USERMGMT_PORT` | 8003 | User management service port |
| `MAX_USERNAME_LENGTH` | 50 | usermgmt-ts: `username` の最大文字数 |
| `USERS_DEFAULT_LIMIT` | 50 | usermgmt-ts: `GET /api/users` の既定ページサイズ |
| `USERS_MAX_LIMIT` | 200 | usermgmt-ts: `GET /api/users` の `limit` 上限 |
| `MAX_SEARCH_LENGTH` | 100 | usermgmt-ts: `GET /api/users` の検索クエリ `q` の最大文字数 |

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
