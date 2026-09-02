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

> 各サービスの内部構造・共通ポリシー (集計エンドポイントの命名規約 / リクエスト観測性 / 意図的な非採用事項) の詳細は [`docs/architecture.md`](docs/architecture.md) を参照してください。

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
| GET | `/api/events/count` | フィルタ後の件数のみを返す軽量エンドポイント（`total` / `distinct_names` / `by_name`）。UI のバッジ表示・ページャ初期化用途で `/summary` より応答が小さい |
| GET | `/api/events/names` | distinct な event_name のみを返す軽量エンドポイント（フィルタドロップダウン / オートコンプリート用） |
| GET | `/api/events/names/<name>` | 単一 event_name の詳細ドリルダウン。`count` / `first_seen` / `last_seen` / `latest_properties` / `distinct_property_keys` を 1 リクエストで返す。0 件なら 404 |
| GET | `/api/events/property_keys` | フィルタ後のイベントに登場した properties キー一覧（`event_name` / `q` / `since` / `until` / `order` / `limit` / `offset`） |
| GET | `/api/events/property_values/<key>` | 指定キーの distinct 値とその出現回数（`event_name` / `q` / `since` / `until` / `sort=value\|count` / `order` / `limit` / `offset`、既定は `count desc`） |
| GET | `/api/events/by_day` | UTC 日付 (`YYYY-MM-DD`) 別の時系列カウント。populated-only で日付昇順 |
| GET | `/api/events/by_week` | ISO 8601 週 (`YYYY-Www`) 別の時系列カウント。日次より粗く月次より細かい中間解像度、populated-only で週昇順 |
| GET | `/api/events/by_month` | UTC 月 (`YYYY-MM`) 別の時系列カウント。長期トレンド用途、populated-only で月昇順 |
| GET | `/api/events/by_hour_of_day` | UTC 時刻 (`00`〜`23`) 別の周期分布。1 日内の流量集中を把握、populated-only で時間順 |
| GET | `/api/events/by_day_of_week` | ISO 曜日 (`1`=Mon〜`7`=Sun) 別の周期分布。平日 vs 週末の傾向把握、populated-only で曜日順 |

**`GET /api/events` query parameters:**
- `event_name`: 完全一致でイベント名を絞り込み
- `limit` / `offset`: ページネーション（`limit` 既定 `DEFAULT_PAGE_LIMIT`、上限 `MAX_PAGE_LIMIT`）
- `since` / `until`: ISO 8601 / RFC 3339 タイムスタンプで期間絞り込み（`since` ≤ `until`）
- `sort`: `timestamp`（既定）/ `event_name`
- `order`: `asc`（既定）/ `desc`

**`GET /api/events/summary` query parameters:** `event_name` / `since` / `until`（`/api/events` と同じ意味）

**`GET /api/events/count` query parameters:** `event_name` / `q` / `since` / `until`（`/api/events` と同じ意味）

**`GET /api/events/names` query parameters:**
- `q`: event_name の大文字小文字無視部分一致
- `since` / `until`: ISO 8601 タイムスタンプ範囲フィルタ（`/api/events` と同じパース）
- `order`: `asc`（既定）/ `desc`（event_name 名昇順 / 降順）
- `limit` / `offset`: `DEFAULT_PAGE_LIMIT` / `MAX_PAGE_LIMIT` を流用

**`GET /api/events/names/<name>` query parameters:** `since` / `until`（`/api/events` と同じ意味）

**`GET /api/events/by_day` / `by_week` / `by_month` / `by_hour_of_day` / `by_day_of_week` query parameters:** `event_name` / `q` / `since` / `until`（`/api/events` と同じ意味）

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

# Daily / weekly / monthly trend
curl http://localhost:8001/api/events/by_day
curl http://localhost:8001/api/events/by_week
curl http://localhost:8001/api/events/by_month
```

### Processor Service (`:8002`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/api/messages` | Publish a message to a channel |
| GET | `/api/messages` | List messages with filtering / pagination / sorting (see params below) |
| GET | `/api/messages/channels` | distinct な channel 名一覧を返す軽量エンドポイント（ドロップダウン populate 用途）。`q` / `since` / `until` / `order` / `limit` / `offset` を受け付ける |
| GET | `/api/messages/count` | フィルタ後の件数のみを返す軽量エンドポイント（`total` / `distinct_channels` / `by_channel`）。UI のバッジ表示・ページャ初期化用途で `/api/stats` より応答が小さい (analytics-py `/api/events/count` / usermgmt-ts `/api/users/count` と対称) |
| GET | `/api/messages/by_day` | UTC 日付 (`YYYY-MM-DD`) 別の時系列カウント。populated-only で日付昇順 |
| GET | `/api/messages/by_week` | ISO 8601 週 (`YYYY-Www`) 別の時系列カウント。日次より粗く月次より細かい中間解像度、populated-only で週昇順 |
| GET | `/api/messages/by_month` | UTC 月 (`YYYY-MM`) 別の時系列カウント。長期トレンド用途、populated-only で月昇順 |
| GET | `/api/messages/by_hour_of_day` | UTC 時刻 (`00`〜`23`) 別の周期分布。1 日内の流量集中を把握、populated-only で時間順 |
| GET | `/api/messages/by_day_of_week` | ISO 曜日 (`1`=Mon〜`7`=Sun) 別の周期分布。曜日別キャパシティプランニング用途、populated-only で曜日順 |
| GET | `/api/messages/{id}` | Get a single message by ID（該当なしは `404`） |
| DELETE | `/api/messages` | `channel` / `since` / `before` の AND で一致するメッセージを削除（少なくとも 1 フィルタ必須。`since` は包含、`before` は排他で半開区間 `[since, before)` を表現） |
| DELETE | `/api/messages/{id}` | 単一メッセージを ID 指定で削除。レスポンスに削除前のメッセージ内容を含め、別 GET なしで監査ログに残せる。該当なしは `404` |
| GET | `/api/stats` | Message count per channel（`?channel=` / `?q=` / `?since=` / `?until=` / `?top_channels_limit=` でフィルタ後の集計を返す） |

**`GET /api/messages` query parameters:**
- `channel`: 完全一致でチャンネルを絞り込み
- `q`: `channel` / `payload` の大文字小文字無視の部分一致（最大 `MAX_SEARCH_LENGTH` 文字）
- `limit` / `offset`: ページネーション（`limit` 既定 `DEFAULT_PAGE_LIMIT`、上限 `MAX_PAGE_LIMIT`）
- `since` / `until`: ISO 8601 / RFC 3339 タイムスタンプで期間絞り込み（`until` ≥ `since`）
- `sort`: `created_at`（既定）/ `channel` / `id`
- `order`: `asc`（既定）/ `desc`

**`GET /api/messages/channels` query parameters:**
- `channel` / `q` / `since` / `until`: `/api/messages` と同じセマンティクスでフィルタ後の distinct を取る
- `order`: `asc`（既定）/ `desc`（channel 名昇順 / 降順）
- `limit` / `offset`: `DEFAULT_PAGE_LIMIT` / `MAX_PAGE_LIMIT` を流用

**`GET /api/messages/count` query parameters:** `channel` / `q` / `since` / `until`（`/api/messages` と同じ意味）。レスポンスは `total`（フィルタ通過後の合計件数）/ `distinct_channels`（登場した channel のユニーク数）/ `by_channel`（channel → count の map、`/api/stats` の `channels` フィールドと同形式）の 3 フィールドのみ。GET 以外は 405、`since > until` や不正な時刻・100 文字超の `q` は 400。

**`GET /api/messages/by_day` / `by_week` / `by_month` / `by_hour_of_day` / `by_day_of_week` query parameters:** `channel` / `q` / `since` / `until`（`/api/messages` と同じ意味）

**`GET /api/stats` query parameters:** `channel` / `q` / `since` / `until` を `/api/messages` と同じセマンティクスで受け付け、フィルタ後のメッセージから集計値を返す。`top_channels_limit` (既定 `5`、上限 `100`) で `top_channels` の件数を制御できる。GET 以外のメソッドは 405。

レスポンス形：

```json
{
  "total_messages": 12,
  "channels": {"alerts": 9, "info": 3},
  "distinct_channels": 2,
  "oldest": "2030-01-01T00:00:00Z",
  "newest": "2030-12-31T23:59:59Z",
  "top_channels": [
    {"channel": "alerts", "count": 9},
    {"channel": "info", "count": 3}
  ]
}
```

- `distinct_channels`: フィルタ通過後に登場した channel のユニーク数（`channels` マップのキー数と一致）
- `oldest` / `newest`: フィルタ通過後の `created_at` の最小・最大(RFC 3339)。マッチ 0 件のときは両方とも `null`。クライアントが追加クエリ無しに「いまフィルタ条件で残っているデータの時間範囲」を把握できる
- `top_channels`: `count` 降順（同数はチャネル名昇順）で先頭 `top_channels_limit` 件。`channels` マップから UI 側で再計算せず、そのまま「上位 N チャネル」バーチャートに使える

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

# Distinct channels for a dropdown
curl http://localhost:8002/api/messages/channels

# Lightweight count (total / distinct_channels / by_channel) for badges and pager init
curl http://localhost:8002/api/messages/count
curl "http://localhost:8002/api/messages/count?channel=alerts&since=2030-01-01T00:00:00Z"

# Daily / weekly / monthly trend
curl http://localhost:8002/api/messages/by_day
curl http://localhost:8002/api/messages/by_week
curl http://localhost:8002/api/messages/by_month
```

### User Management Service (`:8003`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/api/users` | Create a user |
| GET | `/api/users` | List users with filtering / search / pagination / sorting (see params below) |
| GET | `/api/users/count` | ユーザ件数集計（`role` / `q` / `since` / `until` フィルタと `by_role` 内訳を返す軽量エンドポイント） |
| GET | `/api/users/by_day` | UTC カレンダー日 (`YYYY-MM-DD`) ごとのユーザ登録件数集計。populated-only で日付昇順 |
| GET | `/api/users/by_month` | UTC カレンダー月 (`YYYY-MM`) ごとのユーザ登録件数集計。日次より粗い月次トレンド用途、populated-only で月昇順 |
| GET | `/api/users/by_week` | ISO 8601 週 (`YYYY-Www`) ごとのユーザ登録件数集計。日次と月次の中間解像度、四半期・半期スパンの登録推移用途、populated-only で週昇順 |
| GET | `/api/users/by_hour_of_day` | UTC 時刻 (`00`〜`23`) ごとのユーザ登録件数集計 |
| GET | `/api/users/by_day_of_week` | ISO 曜日 (`1`=Mon〜`7`=Sun) ごとのユーザ登録件数集計 |
| GET | `/api/users/by_domain` | email の `@` 以降を小文字化したドメイン別のユーザ件数集計。B2B SaaS のワークスペース単位採用トラッキング / テストドメイン混入検知に使う。populated-only でドメイン名昇順 |
| GET | `/api/users/:id` | Get user by ID |
| PUT | `/api/users/:id` | Update a user (partial update) |
| DELETE | `/api/users/:id` | Delete a user |

**`GET /api/users` query parameters:**
- `limit` / `offset`: ページネーション（`limit` 既定 `USERS_DEFAULT_LIMIT`、上限 `USERS_MAX_LIMIT`）
- `role`: `user` / `admin` / `moderator` で絞り込み
- `q`: `username` / `email` の部分一致検索（大文字小文字を無視）
- `sort`: `created_at`（既定）/ `updated_at` / `username` / `email` / `role`
- `order`: `asc`（既定）/ `desc`

**`GET /api/users/count` / `by_day` / `by_week` / `by_month` / `by_hour_of_day` / `by_day_of_week` / `by_domain` query parameters:** `role` / `q` / `since` / `until` を `/api/users` と同じセマンティクスで受け付ける（`limit` / `offset` / `sort` / `order` は集計エンドポイントでは無視される）。

**Validation rules (POST / PUT):**
- `username`: 必須（POSTのみ）、トリム後 1〜`MAX_USERNAME_LENGTH`（既定 50）文字
- `email`: 必須（POSTのみ）、トリム後 1〜254 文字、簡易メール形式チェック、**小文字に正規化**して保存(大文字違いの重複を防止)
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

# Distribution by email domain
curl http://localhost:8003/api/users/by_domain
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
    │   ├── requirements-dev.txt
    │   ├── test_app.py
    │   └── test_middleware.py
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

## Documentation

`docs/` 配下に、開発・運用・障害対応のためのドキュメントを整理しています。

- [`docs/architecture.md`](docs/architecture.md) — 各サービスの内部構造・共通ポリシー（集計エンドポイントの命名規約 / リクエスト観測性 / 意図的な非採用事項）
- [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) — 障害発生時の症状別切り分け・復旧手順
- [`docs/FAQ.md`](docs/FAQ.md) — 設定・運用・仕様に関するよくある質問と回答

コントリビュートのガイドは [`CONTRIBUTING.md`](CONTRIBUTING.md)、コミュニティ規範は [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)、セキュリティ報告は [`SECURITY.md`](SECURITY.md) を参照してください。

## License

MIT
