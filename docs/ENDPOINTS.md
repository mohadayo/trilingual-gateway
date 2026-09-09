# エンドポイント横断リファレンス

Trilingual Gateway を構成する 3 サービス (analytics-py / processor-go / usermgmt-ts) が公開している HTTP エンドポイントを **1 枚で** 見渡すためのリファレンスです。個別の詳細仕様 (クエリパラメータの完全な意味 / バリデーションルール / レスポンス形) はリポジトリルート [`README.md`](../README.md) の各サービスセクションが一次情報源です。本ドキュメントは、以下のユースケースを対象にしています。

- 集計系エンドポイント (`/count` / `/by_day` / `/by_week` / `/by_month` / `/by_hour_of_day` / `/by_day_of_week`) が **3 サービスにそれぞれ何本ずつ生えているか** を横断で確認したい
- 削除系 (`DELETE`) が **どのサービスに / どの粒度で** 生えているかを見比べたい
- 新しい集計エンドポイントを実装する前に、他サービスの **命名・シグネチャ規約** を隣接参照したい

> クエリパラメータ (`q` / `since` / `until` / `limit` / `offset` / `sort` / `order`) の共通セマンティクスは [`docs/GLOSSARY.md`](./GLOSSARY.md) を参照してください。各サービス固有のパラメータ・バリデーション・レスポンス形は本ドキュメントでは重複記載せず、ルート [`README.md`](../README.md) にリンクします。

## サービスとポート

| サービス | 言語 / フレームワーク | ポート | 主な責務 |
|---------|------------------|------|--------|
| `analytics-py` | Python 3.12 / Flask | `:8001` | イベントトラッキング / 集計 |
| `processor-go` | Go 1.22 / `net/http` | `:8002` | チャネル別リアルタイムメッセージ処理 |
| `usermgmt-ts` | TypeScript / Express | `:8003` | ユーザ CRUD / メール一意性強制 |

各サービスは `GET /health` を共通で公開しています。

## エンドポイント一覧

### analytics-py (`:8001`)

詳細: [ルート README - Analytics Service](../README.md#analytics-service-8001)

| Method | Path | カテゴリ | 目的 |
|--------|------|--------|------|
| GET | `/health` | health | ヘルスチェック |
| POST | `/api/events` | write | イベントを記録 |
| GET | `/api/events` | list | フィルタ / ページネーション / ソート付き一覧 |
| DELETE | `/api/events` | delete | `event_name` 指定で一括削除 |
| GET | `/api/events/summary` | aggregate | event_name 別カウント (旧来集計) |
| GET | `/api/events/count` | aggregate | 軽量カウント (`total` / `distinct_names` / `by_name`) |
| GET | `/api/events/names` | aggregate | distinct な event_name 一覧 (ドロップダウン用途) |
| GET | `/api/events/names/<name>` | drilldown | 単一 event_name の詳細 (`first_seen` / `last_seen` / `latest_properties` 等) |
| GET | `/api/events/property_keys` | aggregate | properties キー一覧 |
| GET | `/api/events/property_values/<key>` | aggregate | 指定キーの distinct 値と出現回数 |
| GET | `/api/events/by_day` | time-series | UTC 日別カウント |
| GET | `/api/events/by_week` | time-series | ISO 8601 週別カウント |
| GET | `/api/events/by_month` | time-series | UTC 月別カウント |
| GET | `/api/events/by_hour_of_day` | periodic | 時刻 (`00`-`23`) 別 |
| GET | `/api/events/by_day_of_week` | periodic | ISO 曜日 (`1`=Mon-`7`=Sun) 別 |

### processor-go (`:8002`)

詳細: [ルート README - Processor Service](../README.md#processor-service-8002)

| Method | Path | カテゴリ | 目的 |
|--------|------|--------|------|
| GET | `/health` | health | ヘルスチェック |
| POST | `/api/messages` | write | チャネルにメッセージを publish |
| GET | `/api/messages` | list | フィルタ / ページネーション / ソート付き一覧 |
| GET | `/api/messages/{id}` | get | ID 指定で 1 件取得 (該当なしは 404) |
| DELETE | `/api/messages` | delete | `channel` / `since` / `before` の AND で一括削除 |
| DELETE | `/api/messages/{id}` | delete | ID 指定で 1 件削除 (削除前レコードをレスポンスに含める) |
| GET | `/api/messages/channels` | aggregate | distinct な channel 一覧 |
| GET | `/api/messages/count` | aggregate | 軽量カウント (`total` / `distinct_channels` / `by_channel`) |
| GET | `/api/messages/by_day` | time-series | UTC 日別カウント |
| GET | `/api/messages/by_week` | time-series | ISO 8601 週別カウント |
| GET | `/api/messages/by_month` | time-series | UTC 月別カウント |
| GET | `/api/messages/by_hour_of_day` | periodic | 時刻 (`00`-`23`) 別 |
| GET | `/api/messages/by_day_of_week` | periodic | ISO 曜日別 |
| GET | `/api/stats` | aggregate | 従来集計 (`total_messages` / `channels` / `top_channels` 等) |

### usermgmt-ts (`:8003`)

詳細: [ルート README - User Management Service](../README.md#user-management-service-8003)

| Method | Path | カテゴリ | 目的 |
|--------|------|--------|------|
| GET | `/health` | health | ヘルスチェック |
| POST | `/api/users` | write | ユーザ作成 (email は小文字に正規化) |
| GET | `/api/users` | list | フィルタ / 検索 / ページネーション / ソート付き一覧 |
| GET | `/api/users/:id` | get | ID 指定で 1 件取得 |
| PUT | `/api/users/:id` | write | 部分更新 |
| DELETE | `/api/users/:id` | delete | ID 指定で 1 件削除 |
| GET | `/api/users/count` | aggregate | 件数集計 (`by_role` 内訳付き) |
| GET | `/api/users/by_day` | time-series | UTC 日別の登録件数 |
| GET | `/api/users/by_week` | time-series | ISO 8601 週別の登録件数 |
| GET | `/api/users/by_month` | time-series | UTC 月別の登録件数 |
| GET | `/api/users/by_hour_of_day` | periodic | 時刻別の登録件数 |
| GET | `/api/users/by_day_of_week` | periodic | ISO 曜日別の登録件数 |
| GET | `/api/users/by_domain` | aggregate | email ドメイン別の件数集計 |

## 集計エンドポイント対応マトリクス

3 サービスは可能な限り **命名を揃える** 方針で集計エンドポイントを整備しています。以下は共通 API と、各サービス固有の集計 API の対応状況です。

| 集計 API | analytics-py | processor-go | usermgmt-ts | 備考 |
|---------|:-----------:|:-----------:|:-----------:|------|
| `/count` | O (`/api/events/count`) | O (`/api/messages/count`) | O (`/api/users/count`) | 軽量カウント。UI バッジ / ページャ初期化用途 |
| `/by_day` | O | O | O | UTC 日 (`YYYY-MM-DD`) 別、populated-only |
| `/by_week` | O | O | O | ISO 8601 週 (`YYYY-Www`) 別、populated-only |
| `/by_month` | O | O | O | UTC 月 (`YYYY-MM`) 別、populated-only |
| `/by_hour_of_day` | O | O | O | 時刻 (`00`-`23`) 別の周期分布 |
| `/by_day_of_week` | O | O | O | ISO 曜日 (`1`=Mon-`7`=Sun) 別の周期分布 |
| `/summary` (旧来集計) | O (`/api/events/summary`) | O (`/api/stats`) | - | usermgmt-ts は `/count` の `by_role` で代替 |
| distinct 一覧 | O (`/api/events/names`) | O (`/api/messages/channels`) | - | 主キーとなる `event_name` / `channel` の distinct 列挙 |
| ドリルダウン | O (`/api/events/names/<name>`) | - | - | analytics 固有: 単一 event_name の詳細 |
| プロパティ集計 | O (`/api/events/property_keys` / `/api/events/property_values/<key>`) | - | - | analytics 固有: 動的 properties キー空間の探索 |
| ドメイン集計 | - | - | O (`/api/users/by_domain`) | usermgmt 固有: email `@` 以降別 |

> `O` = 実装あり / `-` = 実装なし (責務外 or 別 API で代替)

新しい集計エンドポイントを追加する際は、上記命名規約に沿うことで、[`docs/GLOSSARY.md`](./GLOSSARY.md) の共通クエリ規約と一貫した挙動を維持できます。

## 共通仕様

以下は 3 サービスに共通する挙動です。詳細は各リンク先を参照してください。

- **クエリパラメータ**: `q` / `since` / `until` / `limit` / `offset` / `sort` / `order` — 意味と既定値は [`docs/GLOSSARY.md`](./GLOSSARY.md) の「共通クエリパラメータ」節を参照
- **ページネーション上限**: 各サービスの `*_MAX_LIMIT` / `MAX_PAGE_LIMIT` 環境変数で制御 — [ルート README - Environment Variables](../README.md#environment-variables)
- **populated-only**: 時系列 / 周期集計は「1 件以上マッチしたバケット」のみを返し、空バケットは省略
- **タイムスタンプ**: 入出力とも UTC 基準の ISO 8601 / RFC 3339
- **メソッド不一致**: 定義外メソッドは `405 Method Not Allowed`
- **リクエスト観測性**: リクエストごとの構造化ログ — [`docs/architecture.md`](./architecture.md) を参照

## 関連ドキュメント

- [`docs/architecture.md`](./architecture.md) — 各サービスの内部構造・共通ポリシー・意図的な非採用事項
- [`docs/GLOSSARY.md`](./GLOSSARY.md) — 集計エンドポイント / 共通クエリ / 環境変数命名規約の用語集
- [`docs/RUNBOOK.md`](./RUNBOOK.md) — 起動・停止・環境変数切り替えなどの定常運用手順
- [`docs/TROUBLESHOOTING.md`](./TROUBLESHOOTING.md) — サービス間通信 / ルーティング関連の切り分け
- [`docs/FAQ.md`](./FAQ.md) — よくある質問と回答
- ルート [`README.md`](../README.md) — サービスごとの詳細仕様 (クエリ / バリデーション / レスポンス例)
