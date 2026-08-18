# Contributing to Trilingual Gateway / コントリビュートガイド

Trilingual Gateway への貢献を検討いただきありがとうございます。
このドキュメントは、Issue の起票から Pull Request のマージまで、参加者が最初に知りたい情報をまとめたものです。日本語と英語を併記しています。

Thank you for considering a contribution to Trilingual Gateway.
This guide walks you through everything from filing issues to landing a pull request, in both Japanese and English.

---

## 目次 / Table of Contents

1. [行動規範 / Code of Conduct](#行動規範--code-of-conduct)
2. [前提条件 / Prerequisites](#前提条件--prerequisites)
3. [ローカルセットアップ / Local Setup](#ローカルセットアップ--local-setup)
4. [開発ワークフロー / Development Workflow](#開発ワークフロー--development-workflow)
5. [テストと Lint / Testing & Linting](#テストと-lint--testing--linting)
6. [ブランチ命名規約 / Branching Convention](#ブランチ命名規約--branching-convention)
7. [コミット・PR タイトル規約 / Commit & PR Title Convention](#コミットpr-タイトル規約--commit--pr-title-convention)
8. [Pull Request の出し方 / Pull Request Workflow](#pull-request-の出し方--pull-request-workflow)
9. [Issue の切り方 / Filing Issues](#issue-の切り方--filing-issues)
10. [セキュリティ / Security](#セキュリティ--security)

---

## 行動規範 / Code of Conduct

このプロジェクトは [Contributor Covenant](https://www.contributor-covenant.org/) 準拠の [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md) を採用しています。参加者はこれに同意したものとみなします。

This project adopts a [Contributor Covenant](https://www.contributor-covenant.org/) based [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md). By participating, you agree to abide by it.

---

## 前提条件 / Prerequisites

リポジトリで作業するには以下が必要です。バージョンは [`.tool-versions`](./.tool-versions) と揃えてください (asdf / mise で自動的に切り替わります)。

The following are required. Keep versions in sync with [`.tool-versions`](./.tool-versions) — asdf / mise will pick them up automatically.

| ツール / Tool | バージョン / Version | 用途 / Purpose |
|--------------|---------------------|----------------|
| Docker & Docker Compose | latest | 全サービスの一括起動 / Running all services |
| Python | 3.12 | `services/analytics-py` |
| Go | 1.22 | `services/processor-go` |
| Node.js | 20 | `services/usermgmt-ts` |
| GNU Make | 3.81+ | `make` ターゲット / `make` targets |

Docker だけでもフルスタックを起動できます。ローカルで各言語のテストを流したい場合のみ、対応するランタイムをインストールしてください。

Docker alone is enough to bring the whole stack up. Install the language runtimes only when you want to run tests locally.

---

## ローカルセットアップ / Local Setup

```bash
# 1. リポジトリを取得 / Clone the repository
git clone https://github.com/mohadayo/trilingual-gateway.git
cd trilingual-gateway

# 2. 環境変数を用意 / Prepare environment variables
cp .env.example .env

# 3. Docker Compose で全サービスを起動 / Start every service with Docker Compose
make up

# 4. 状態確認 / Check status
make ps
make logs   # ログを追跡 / Tail logs

# 5. 停止 / Stop
make down
```

サービスは以下のポートで待機します。

Services listen on the following ports.

| サービス / Service | ポート / Port | ヘルスチェック / Health |
|------------------|--------------|------------------------|
| analytics-py (Python / Flask) | 8001 | `GET /health` |
| processor-go (Go / net/http) | 8002 | `GET /health` |
| usermgmt-ts (TypeScript / Express) | 8003 | `GET /health` |

各エンドポイントの詳細は [`README.md`](./README.md) の "API Reference" を参照してください。

See "API Reference" in [`README.md`](./README.md) for endpoint details.

---

## 開発ワークフロー / Development Workflow

サービスは `services/` 配下に言語ごとに分かれています。基本は「担当サービスのディレクトリ内で作業し、そのサービスのテスト・Lint をパスしてからコミット」です。

Services live under `services/`, one per language. The basic rule: work inside your target service's directory and make sure its tests and lint pass before committing.

```
services/
├── analytics-py/     # Python 3.12 / Flask
├── processor-go/     # Go 1.22 / net/http
└── usermgmt-ts/      # TypeScript / Express
```

複数サービスにまたがる変更 (API スキーマ変更など) を行う場合は、PR 説明で影響範囲を明記してください。

For changes spanning multiple services (e.g. API schema updates), spell out the impact in the PR description.

---

## テストと Lint / Testing & Linting

すべてリポジトリルートの [`Makefile`](./Makefile) から実行できます。

Everything is wired through the root [`Makefile`](./Makefile).

```bash
# 全サービス / All services
make test          # 全テスト実行 / Run every test suite
make lint          # 全 Lint 実行 / Run every linter

# サービス別 / Per service
make test-python   # pytest (analytics-py)
make test-go       # go test -race (processor-go)
make test-ts       # jest (usermgmt-ts)
```

PR を送る前に、少なくとも触ったサービスの `make test-*` と `make lint` はローカルで通してください。CI (`.github/workflows/ci.yml`) でも同じチェックが走ります。

Before opening a PR, please run at least the `make test-*` and `make lint` for the service(s) you touched. CI (`.github/workflows/ci.yml`) runs the same checks.

---

## ブランチ命名規約 / Branching Convention

`main` から新しいブランチを切って作業してください。ブランチ名はプレフィックス付きが望ましいです。

Branch off `main`. Prefer prefixed branch names.

| プレフィックス / Prefix | 用途 / Purpose |
|------------------------|---------------|
| `feat/*` | 新機能 / New feature |
| `fix/*` | バグ修正 / Bug fix |
| `docs/*` | ドキュメントのみ / Docs only |
| `chore/*` | 雑務・設定変更 / Chores & config |
| `refactor/*` | 挙動を変えないリファクタ / Refactor without behavior change |
| `test/*` | テストの追加・修正 / Test-only changes |
| `claude/*` | Claude が自動生成したブランチ / Branches generated by Claude |

例 / Examples:

```
feat/analytics-percentile-endpoint
fix/processor-goroutine-leak
docs/contributing-guide
```

---

## コミット・PR タイトル規約 / Commit & PR Title Convention

コミットメッセージと PR タイトルは [Conventional Commits](https://www.conventionalcommits.org/) 風のプレフィックスを付けてください。

Prefix commit messages and PR titles in the [Conventional Commits](https://www.conventionalcommits.org/) style.

| プレフィックス / Prefix | 意味 / Meaning |
|------------------------|---------------|
| `feat:` | 新機能 / New feature |
| `fix:` | バグ修正 / Bug fix |
| `docs:` | ドキュメント / Documentation |
| `chore:` | 雑務・設定 / Chores & config |
| `refactor:` | リファクタ / Refactor |
| `test:` | テスト / Tests |
| `perf:` | 性能改善 / Performance improvement |
| `ci:` | CI 変更 / CI changes |

例 / Examples:

```
feat: analytics-py に p95 レイテンシ集計エンドポイントを追加
fix: processor-go の channel クローズ時 panic を修正
docs: README の API 例を最新スキーマに追従
```

日本語・英語どちらでも構いませんが、プレフィックスは半角英字・小文字で統一してください。

Japanese or English is fine; keep the prefix lowercase ASCII.

---

## Pull Request の出し方 / Pull Request Workflow

1. **Draft で作成 / Open as Draft**
   - 作業途中でもレビュアーが早めに方向性を確認できるよう、まずは Draft PR を作成することを推奨します。
   - Prefer opening as a Draft first so reviewers can spot direction issues early.
2. **[`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md) を埋める / Fill in the template**
   - `Closes #<issue番号>` を必ず含める / Always include `Closes #<issue-number>`.
3. **CI を通す / Pass CI**
   - `.github/workflows/` のジョブがすべて緑になるまで Draft のままにしてください。
   - Keep it Draft until every job in `.github/workflows/` is green.
4. **Ready for review に切り替え / Mark as Ready for review**
   - レビュアーが未指定でも、コードオーナー ([`.github/CODEOWNERS`](.github/CODEOWNERS)) が自動アサインされます。
   - Even without an explicit reviewer, code owners in [`.github/CODEOWNERS`](.github/CODEOWNERS) are auto-assigned.
5. **Squash merge**
   - デフォルトのマージ方式は **Squash and merge** です。PR タイトルがそのままコミットメッセージになるので、上記の規約に従ってください。
   - The default merge strategy is **Squash and merge**. The PR title becomes the commit message, so follow the convention above.

---

## Issue の切り方 / Filing Issues

`.github/ISSUE_TEMPLATE/` にテンプレートが用意されています。

Templates live in `.github/ISSUE_TEMPLATE/`.

- **Bug Report** — 再現手順・期待挙動・実際の挙動・環境情報を埋めてください。
  Fill in reproduction steps, expected vs. actual behavior, and environment info.
- **Feature Request** — 解決したい課題 (why) を、実装案 (how) より先に書いてください。
  Lead with the problem you want to solve (why) before proposing an implementation (how).

重複を避けるため、起票前に既存の Open Issue を検索してください。似た Issue があればコメントで補足するほうが議論が集約されます。

Search open issues first to avoid duplicates. If a similar one exists, add a comment there instead — discussion stays consolidated.

---

## セキュリティ / Security

**脆弱性を公開 Issue に投稿しないでください。** 報告手順は [`SECURITY.md`](./SECURITY.md) を参照してください。

**Do not open public issues for security vulnerabilities.** See [`SECURITY.md`](./SECURITY.md) for the reporting process.

---

## ライセンス / License

コントリビュートいただいたコードは、リポジトリと同じ [MIT License](./LICENSE) の下で公開されます。

Contributions are released under the same [MIT License](./LICENSE) as the repository.
