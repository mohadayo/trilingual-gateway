# よくある質問 (FAQ)

`trilingual-gateway` を使い始めるとき / 運用するときに繰り返し聞かれる質問と回答を集約する。

- システム全体像は `docs/architecture.md`
- 具体的な障害調査手順は `docs/TROUBLESHOOTING.md`

本 FAQ は "最初に読むべき所" として、決定済みの方針や再発する疑問を短くまとめる。詳細な調査ログはここに書かず、各ドキュメントに委ねる。

## 目次

- [セットアップ・起動](#セットアップ起動)
- [設定・環境変数](#設定環境変数)
- [開発ワークフロー](#開発ワークフロー)
- [テストと CI](#テストと-ci)
- [3 言語混在ゆえのハマりどころ](#3-言語混在ゆえのハマりどころ)
- [運用・観測](#運用観測)
- [ドキュメント間の分担](#ドキュメント間の分担)

## セットアップ・起動

### Q. 最短で全サービスを立ち上げるには?

```bash
cp .env.example .env
docker compose up -d
make health
```

`make health` が全サービスで OK を返せば起動完了。個別サービスの詳細は `services/<name>/README.md` を参照する。

### Q. 一部サービスだけ起動して手元の言語で開発したい

Docker Compose で依存だけ立ち上げてから、対象サービスは手元で走らせる。

```bash
docker compose up -d <deps...>
cd services/<target>
make run  # 各サービスの Makefile ターゲットに従う
```

### Q. `docker compose up` が起動途中で止まる

`docker compose logs` で失敗しているサービスを特定する。多くは `.env` 未設定 (未定義キーが `${VAR}` 展開で失敗) が原因。`.env.example` と `.env` の差分を確認する。

## 設定・環境変数

### Q. 環境変数はどこで一元管理される?

`.env.example` が全キーの正である。新しい環境変数を追加した場合はここへ必ずコメント付きで追記する。`.env` は個人ローカル (git 管理外)。

### Q. サービス間で共有する設定と、サービス固有の設定を分けたい

- 共有: ルート `.env` に定義し、`docker-compose.yml` から各サービスに `environment:` で注入
- 固有: `services/<name>/config/` 配下に定義。ルート `.env` を汚染しない

### Q. 秘密情報 (API キー等) はどう扱う?

`.env` にのみ保存し、`.env.example` にはダミー値または空値でキーの存在を示す。本番環境では Secret Manager から注入する。ソースにハードコードしない。

## 開発ワークフロー

### Q. どのブランチから作業を始めればいい?

`main` から作業ブランチを切る。命名は `feat/` `fix/` `refactor/` `test/` `docs/` `chore/` のプレフィックス + 短い要約 (例: `feat/add-rate-limit-metric`)。

### Q. コミットメッセージのフォーマットは?

`CONTRIBUTING.md` に従う。Conventional Commits 系のプレフィックス + 日本語で簡潔に。1 コミット 1 関心事。

### Q. PR を出す前に必ずやることは?

- ルート `make check` (各サービスの lint / format / test を横断で実行)
- CHANGELOG に相応のエントリを追加 (機能追加・破壊的変更・修正)
- 依存追加時はライセンス互換性を確認

## テストと CI

### Q. 各言語のテストコマンドは?

| サービス | 言語 | テストコマンド |
| :-- | :-- | :-- |
| analytics | Python | `pytest -v` (services/analytics 配下) |
| real-time | Go | `go test -v ./...` (services/real-time 配下) |
| user-management | TypeScript | `npm test` (services/user-management 配下) |

CI で実行される正確なコマンドは `.github/workflows/ci.yml` を参照する。

### Q. CI が落ちるがローカルでは通る

以下を順に確認する:

1. Node/Python/Go のバージョンが `.tool-versions` と一致しているか (`asdf install` 推奨)
2. 依存ロックが最新か (`npm ci` は `package-lock.json` に厳密、`pip install -r requirements.txt` は解決が異なる可能性)
3. タイムゾーン (テストが `TZ` に依存していないか)
4. キャッシュ (`docker compose build --no-cache` で再現)

### Q. CI をローカルで再現したい

CI の各ステップは `.github/workflows/ci.yml` の `run:` に書いてある。上から順に手元で叩けば同じ結果になる (依存インストール含めて)。

## 3 言語混在ゆえのハマりどころ

### Q. サービス間 API の型定義がずれる

言語ごとに手書きの型がドリフトしがち。JSON Schema や Protobuf をルート `schemas/` に置き、各言語の生成器で型を派生させる方針を守る。

### Q. ログの形式がサービスごとに違って読みにくい

全サービスで JSON 1 行を stdout に出す。必須フィールド (`ts` / `level` / `service` / `msg` / `request_id`) を各言語のロガーで固定する (Python: structlog / Go: zerolog or slog / Node: pino など)。

### Q. `X-Request-Id` の伝播が抜けている

サービス境界を跨ぐ通信で `X-Request-Id` を受け取り、ログとレスポンスに載せる。ミドルウェアで自動化し、個別実装しない。

## 運用・観測

### Q. どこで全サービスの健全性を見られる?

`make health` で全サービスの `/healthz` を一括叩く。継続的な観測は `/metrics` を Prometheus 形式で公開しているので、外部のダッシュボードに集約する。

### Q. 特定リクエストを横断で追跡したい

`request_id` (`X-Request-Id`) をキーに、`docker compose logs | jq 'select(.request_id == "...")'` でサービスを跨いだ相関が可能。

### Q. 障害調査の入り口は?

まず `docs/TROUBLESHOOTING.md` を読む。当てはまらなければ、ゴールデンシグナル (レイテンシ / トラフィック / エラー / サチュレーション) をダッシュボードで並べて影響範囲を切り分ける。

## ドキュメント間の分担

| 目的 | 参照先 |
| :-- | :-- |
| 決定済みの前提・繰り返し質問 | `docs/FAQ.md` (本ファイル) |
| 全体設計・データフロー | `docs/architecture.md` |
| 個別の障害調査手順 | `docs/TROUBLESHOOTING.md` |
| コントリビュートの流儀 | `CONTRIBUTING.md` |
| セキュリティ報告経路 | `SECURITY.md` |

FAQ は "定着した合意" を短く書く場所。実装詳細や調査ログはここには置かない。

## 変更履歴

- 2026-08: 初版作成。
