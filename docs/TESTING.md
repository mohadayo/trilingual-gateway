# テスト（TESTING）

Trilingual Gateway は **3 サービス** がそれぞれ別のテストランナー・lint ツールで検証されます。ローカル検証と CI (`.github/workflows/ci.yml`) の対応関係・各サービス固有の勘所をここに集約します。

Makefile / CI ワークフロー / `CONTRIBUTING.md` に散らばっていた情報の統合インデックスとして参照してください。

## サービスとテストランナー

| サービス | 言語 | テストランナー | Lint | 代表コマンド（Makefile） |
|---|---|---|---|---|
| `services/analytics-py/` | Python | `pytest` | `flake8` | `make test-python` |
| `services/processor-go/` | Go | 標準 `go test -race` | `go vet` | `make test-go` |
| `services/usermgmt-ts/` | TypeScript (Node) | `jest`（`npm test`） | `eslint` | `make test-ts` |

### なぜ Go だけ `-race` を既定にしているか

Go サービス (`processor-go`) は gateway のリアルタイム処理層を担っており、goroutine を使った並行処理が主役です。race detector を標準オフにしていると、同時アクセスで初めて顕在化するデータ競合がプロダクションまで届いてしまいます。コストはテスト時間 2〜10 倍程度ですが、`make test-go` と CI の両方で `-race` を既定化することで、PR の段階で検知する運用に揃えています。

Python / TypeScript 側は競合安全性のモデルが異なる（GIL / event loop 単一スレッド）ため、`-race` 相当は不要です。並行アクセスを検証したい場合は `pytest-asyncio` / `asyncio.gather` / `Promise.all` を使った統合テストで表現します。

### Lint 設定の集約先

- Python: `services/analytics-py/requirements-dev.txt` に `flake8` を入れ、`.flake8`（既存のものがあれば）に合わせる
- Go: 標準 `go vet`（追加設定なし）
- TypeScript: `services/usermgmt-ts/.eslintrc*`（`npx eslint src/` が対象）

## ローカルで CI と等価な検証を行う

CI (`.github/workflows/ci.yml`) は複数ジョブで構成されており、Makefile の対応ターゲットで CI の各ステップを再現できます：

```sh
make test        # test-python + test-go + test-ts
make lint        # 3 サービス全部の lint
```

CI の docker build 相当：

```sh
make build       # docker compose build
```

push 前の最終確認は以下 1 行で CI 失敗の多くを先取り検知できます：

```sh
make lint test build
```

## 新しいテストを追加する時のチェックリスト

### Python (`services/analytics-py/`)

- [ ] テストファイルは `test_*.py` または `*_test.py`（`pytest` のデフォルト discovery に合わせる）
- [ ] テスト用の追加依存は `requirements-dev.txt` に追加する（本番 `requirements.txt` を汚さない）
- [ ] `flake8 --max-line-length=120` を通す
- [ ] 時刻依存・HTTP I/O はモック化し、CI での flakiness を避ける

### Go (`services/processor-go/`)

- [ ] テストファイル名は `<対象>_test.go`、関数は `TestXxx(t *testing.T)` の規約に従う
- [ ] **並行処理を伴うコードを追加した時は特に `-race` が通ることを必ず手元で確認する**（`make test-go` と同条件）
- [ ] テーブル駆動テスト + `t.Run(name, ...)` のサブテスト名でケースを識別可能にする
- [ ] `go vet ./...` を通す

### TypeScript (`services/usermgmt-ts/`)

- [ ] テストは `src/**/*.test.ts` または `__tests__/` に配置する（`jest` のデフォルト設定）
- [ ] 型エラーをテストで隠さない（`as any` でなく `satisfies` or 型拡張で解く）
- [ ] モックは `jest.mock(...)` を使い、`beforeEach` で `jest.resetModules()` / `jest.clearAllMocks()` を呼んで副作用漏れを防ぐ
- [ ] `npm ci` はロックファイル厳密モード。依存追加時は `package-lock.json` のコミットを忘れない

### 共通

- [ ] CI の 3 ジョブすべてが新規テストで緑であることを `make test` でローカル確認してから push する
- [ ] 外部サービス（Redis / PostgreSQL 等）に依存する統合テストは `docker-compose.yml` と整合させ、必要なら `make up` でローカル環境を立ち上げてから走らせる

## 関連ドキュメント

- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — ブランチ運用・コミット規則・レビューの流れ
- [`./architecture.md`](./architecture.md) — 3 サービスの責務とサービス間通信（テスト境界設計の参照先）
- [`./ENDPOINTS.md`](./ENDPOINTS.md) — サービスごとの HTTP エンドポイント一覧（統合テストの観点整理に使える）
- [`./TROUBLESHOOTING.md`](./TROUBLESHOOTING.md) — テスト以外の運用で発生しがちな事象の切り分け
- [`./GLOSSARY.md`](./GLOSSARY.md) — テスト関連で登場する用語の定義
- [`../Makefile`](../Makefile) — 本ドキュメントが参照する全ターゲットの一次定義
- [`../.github/workflows/ci.yml`](../.github/workflows/ci.yml) — CI の一次定義
