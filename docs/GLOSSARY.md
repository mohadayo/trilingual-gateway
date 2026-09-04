# Glossary

Trilingual Gateway 全体で使われる用語の定義集です。API リクエスト仕様は
[`../README.md`](../README.md)、内部構造・横断ポリシーは
[`architecture.md`](architecture.md) を参照してください。本ドキュメントは
「用語 → その意味と根拠 1 箇所」の対応表として、レビュー・オンボーディング・
議論中に短時間で参照できることを目的にします。

目次:

- [1. アーキテクチャ用語](#1-アーキテクチャ用語)
- [2. サービス](#2-サービス)
- [3. ドメイン用語: analytics-py](#3-ドメイン用語-analytics-py)
- [4. ドメイン用語: processor-go](#4-ドメイン用語-processor-go)
- [5. ドメイン用語: usermgmt-ts](#5-ドメイン用語-usermgmt-ts)
- [6. 集計エンドポイントの用語](#6-集計エンドポイントの用語)
- [7. 共通クエリパラメータ](#7-共通クエリパラメータ)
- [8. 観測性の用語](#8-観測性の用語)
- [9. 設定・環境変数の命名規約](#9-設定環境変数の命名規約)

---

## 1. アーキテクチャ用語

| 用語 | 意味 |
|---|---|
| **polyglot / trilingual** | 同一プラットフォーム内で複数プログラミング言語を用いる構成。本リポジトリでは Python / Go / TypeScript の 3 言語で類似ドメインを実装し、言語間の書き分け比較を目的とする。 |
| **microservice** | 独立にデプロイ可能な単機能サービス。本リポジトリの 3 サービスは相互通信せず、クライアントが直接それぞれの API を叩く構成 ([architecture.md §2](architecture.md#2-サービス構成))。 |
| **in-memory ストレージ** | プロセス内メモリのみで状態を保持し、DB 等の外部永続化を持たない方式。学習用ベースラインとして依存を最小化するための意図的な選択 ([architecture.md §6](architecture.md#6-意図的な非採用事項-non-goals))。 |
| **graceful shutdown** | SIGTERM 等の停止シグナル受領後、新規リクエストを受け付けず in-flight のリクエストが完了するまで待って終了する挙動。`processor-go` は `SHUTDOWN_TIMEOUT_SECONDS` を上限に `http.Server.Shutdown()` で実装。 |
| **healthcheck** | サービスが稼働中かを外部から確認するための `/health` エンドポイント。`docker compose` の `healthcheck` からポーリングされ、`healthy` / `unhealthy` 状態遷移に使う。 |
| **structured logging (アクセスログ)** | `method path -> status (duration_ms)` 形式の 1 行 INFO ログ。3 サービス共通の意味論で出力する ([architecture.md §4.1](architecture.md#41-リクエスト観測性))。 |

## 2. サービス

| サービス | 言語 / フレームワーク | ポート | 責務 |
|---|---|---|---|
| **analytics-py** | Python 3.12 / Flask | 8001 | イベント記録と各種集計 (`/api/events*`) |
| **processor-go** | Go 1.22 / net/http (標準ライブラリのみ) | 8002 | メッセージ受信と channel 別集計 (`/api/messages*`, `/api/stats`) |
| **usermgmt-ts** | TypeScript / Express | 8003 | ユーザ CRUD と登録推移集計 (`/api/users*`) |

## 3. ドメイン用語: analytics-py

| 用語 | 意味 |
|---|---|
| **event** | analytics-py が記録する 1 件の観測データ。`event_name` / `properties` / サーバ時計で付与する `timestamp` を持つ。 |
| **event_name** | イベントの識別子文字列。トリム後 1〜`MAX_EVENT_NAME_LENGTH` (既定 200) 文字。多くの集計エンドポイントで完全一致フィルタのキーになる。 |
| **properties** | イベント固有のキー / 値マップ (JSON オブジェクト)。任意の追加属性を格納する自由スキーマフィールド。 |
| **eviction (退避)** | `MAX_EVENTS` (既定 10000) を超えた際に古い側から削除する FIFO 型のメモリ上限管理。時間窓ベースではない。 |
| **`/api/events/summary`** | `event_name` ごとの件数集計。ダッシュボードの一次表示向け。 |
| **`/api/events/property_keys` / `property_values/<key>`** | `properties` に登場したキー一覧および値ごとの件数集計。フィルタドロップダウン等の UI 補助用途。 |

## 4. ドメイン用語: processor-go

| 用語 | 意味 |
|---|---|
| **message** | processor-go が受信する 1 件のメッセージ。`channel` / `payload` / サーバ時計で付与する `created_at` / `id` を持つ。 |
| **channel** | メッセージ配信先の論理チャネル名。トリム後 1〜`MAX_CHANNEL_LENGTH` (既定 256) 文字。集計 API の主キーとなる分類軸。 |
| **payload** | メッセージ本文の文字列。トリム後 1〜`MAX_PAYLOAD_LENGTH` (既定 65536) 文字。 |
| **`/api/stats`** | `channels` (channel → count マップ) / `total_messages` / `distinct_channels` / `oldest` / `newest` / `top_channels` を返す集計エンドポイント。 |
| **`top_channels`** | `count` 降順・同数はチャネル名昇順で先頭 `top_channels_limit` 件を返した配列。UI の「上位 N チャネル」バーチャートに直接使える形式。 |

## 5. ドメイン用語: usermgmt-ts

| 用語 | 意味 |
|---|---|
| **user** | usermgmt-ts が管理する 1 件のユーザレコード。`id` / `username` / `email` / `role` / `created_at` / `updated_at` を持つ。 |
| **username** | ユーザの表示用識別子。トリム後 1〜`MAX_USERNAME_LENGTH` (既定 50) 文字。 |
| **email 正規化** | 保存前に email を小文字化する処理。RDB の UNIQUE 制約を用いない代わりに、大文字違いを含めた重複をアプリ層で拒否する。 |
| **role** | ユーザの権限区分。`user` / `admin` / `moderator` のいずれか (既定 `user`)。 |
| **`/api/users/by_domain`** | email の `@` 以降 (小文字化) をキーにしたユーザ件数集計。B2B SaaS のワークスペース単位トラッキング / テストドメイン混入検知が想定用途。 |

## 6. 集計エンドポイントの用語

3 サービスで共通の意味論を持つ用語です ([architecture.md §4.2](architecture.md#42-集計エンドポイントの命名挙動規約))。

| 用語 | 意味 |
|---|---|
| **bucket** | 集計単位。日 / 週 / 月 / 時 / 曜日など時間軸の区切り、あるいは channel / event_name / domain 等の分類軸。 |
| **populated-only** | 母集団 0 のバケットをレスポンスから省略する挙動。連続する空日を返さないため、ゼロ埋めが必要なダッシュボードはクライアント側で「軸生成 → join」して補う。 |
| **distinct** | フィルタ通過後に登場したユニーク値の集合または個数。`distinct_channels` / `distinct_names` / `distinct_property_keys` 等の各種フィールドで使われる。 |
| **`by_day`** | UTC カレンダー日 (`YYYY-MM-DD`) 別の時系列カウント。populated-only で日付昇順。 |
| **`by_week`** | ISO 8601 週 (`YYYY-Www`) 別の時系列カウント。日次と月次の中間解像度、populated-only で週昇順。 |
| **`by_month`** | UTC カレンダー月 (`YYYY-MM`) 別の時系列カウント。長期トレンド用途、populated-only で月昇順。 |
| **`by_hour_of_day`** | UTC 時刻 (`"00"`〜`"23"` の 2 桁ゼロ詰め) 別の周期分布。1 日内の流量集中を把握する用途。 |
| **`by_day_of_week`** | ISO 曜日 (`"1"`=Mon〜`"7"`=Sun) 別の周期分布。平日 vs 週末や曜日別キャパシティプランニング用途。 |
| **`by_domain`** | usermgmt-ts 固有。email の `@` 以降 (小文字化) 別のユーザ件数集計。populated-only でドメイン名昇順。 |
| **`oldest` / `newest`** | フィルタ通過後の `created_at` / `timestamp` の最小・最大 (RFC 3339)。マッチ 0 件のときは `null`。追加クエリなしで「いまフィルタ条件で残っているデータの時間範囲」を把握できる。 |

> **キー形式のゼロ詰め**: `by_hour_of_day` / `by_day_of_week` は 2 桁ゼロ詰めで返し、`by_week` は `YYYY-Www` の 2 桁週番号を使う。これは「lex 順 = カレンダー順」を保つため。

## 7. 共通クエリパラメータ

list / 集計エンドポイントで共通のセマンティクスを持つクエリです ([architecture.md §4.3](architecture.md#43-ページネーションソートフィルタの共通クエリ規約))。

| 用語 | 意味 |
|---|---|
| **`limit`** | ページサイズ。既定と上限はサービスごと (`DEFAULT_PAGE_LIMIT` / `MAX_PAGE_LIMIT` / `USERS_DEFAULT_LIMIT` / `USERS_MAX_LIMIT`) に定義。負値は既定、上限超は暗黙クランプ (400 は返さない)。 |
| **`offset`** | 先頭からスキップする件数。負値は 0 として扱う。 |
| **`since` / `until`** | ISO 8601 / RFC 3339 タイムスタンプによる時間範囲フィルタ。半開区間として扱う `[since, until)` の実装もあるが、原則 `since ≤ until` を要件とし、逆順は 400。 |
| **`before`** | processor-go の `DELETE /api/messages` 専用の排他上限。`since` (包含) と組で半開区間 `[since, before)` を表現する。 |
| **`q`** | 大文字小文字を無視する部分一致フリーテキスト検索。対象フィールドはサービスごとに固定 (analytics-py: `event_name`、processor-go: `channel` / `payload`、usermgmt-ts: `username` / `email`)。最大長は `MAX_SEARCH_LENGTH` (既定 100)。 |
| **`sort` / `order`** | 並び順の指定。`sort` はサービスごとに ALLOWLIST 化 (未許可は 400)、`order` は `asc` (既定) / `desc`。 |
| **`top_channels_limit`** | processor-go `/api/stats` 専用。`top_channels` 配列の長さ上限 (既定 5、上限 100)。 |

## 8. 観測性の用語

| 用語 | 意味 |
|---|---|
| **`X-Response-Time-Ms`** | サーバ側で計測したレスポンス処理時間 (ミリ秒、小数 3 桁) を返すレスポンスヘッダ。3 サービス共通で提供する。BFF / SPA / APM 集約への利用を想定。 |
| **単調増加時計** | `time.perf_counter_ns` (Python) / `time.Now` (Go) / `process.hrtime.bigint` (Node.js) 等、システムタイムジャンプ (NTP 修正 / DST 切替) の影響を受けない計測時計。アクセスログ / `X-Response-Time-Ms` の計測に使う。 |
| **access log (1 行フォーマット)** | `method path -> status (duration_ms)` の 1 行 INFO ログ。3 サービス共通の意味論。パースしやすく、テストからも文字列マッチで検証しやすい形式にしている。 |

## 9. 設定・環境変数の命名規約

| パターン | 意味 | 例 |
|---|---|---|
| **`<SERVICE>_PORT`** | サービスの待ち受けポート | `ANALYTICS_PORT` / `PROCESSOR_PORT` / `USERMGMT_PORT` |
| **`MAX_*`** | 件数・長さの上限 | `MAX_EVENTS` / `MAX_PAGE_LIMIT` / `MAX_PAYLOAD_SIZE` / `MAX_CHANNEL_LENGTH` / `MAX_SEARCH_LENGTH` |
| **`DEFAULT_*`** | 既定値 | `DEFAULT_PAGE_LIMIT` |
| **`*_TIMEOUT` / `*_TIMEOUT_SECONDS` / `*_TIMEOUT_MS`** | タイムアウト値 (単位はサフィックスで明示) | `PROCESSOR_READ_HEADER_TIMEOUT` / `PROCESSOR_READ_TIMEOUT` / `PROCESSOR_WRITE_TIMEOUT` / `PROCESSOR_IDLE_TIMEOUT` / `SHUTDOWN_TIMEOUT_SECONDS` |

新しい環境変数を追加する際は必ず [`../.env.example`](../.env.example) と README の
"Environment Variables" 表の両方に反映してください ([architecture.md §4.4](architecture.md#44-環境変数命名規約))。

---

> 用語が本ドキュメントに未登録である・意味が実装と乖離しているなどのフィードバックは Issue で歓迎します。定義の変更を伴う PR は必ず本ドキュメントと `docs/architecture.md` の該当節を同時に更新してください。
