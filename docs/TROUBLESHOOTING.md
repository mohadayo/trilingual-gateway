# トラブルシューティング

Trilingual Gateway (`analytics-py` / `processor-go` / `usermgmt-ts`) の
ローカル開発 / Docker Compose 実行時によく遭遇する問題と対処方法をまとめています。

全体構成は [architecture.md](./architecture.md) を、開発フローは
[../CONTRIBUTING.md](../CONTRIBUTING.md) を併せてご参照ください。

## 1. `docker compose up` が失敗する

### 症状

```
Error response from daemon: ... port is already allocated
```

または特定のサービスコンテナが起動直後に exit する。

### 原因と対処

- **ポート競合**: `docker-compose.yml` で公開しているポートが
  ホスト側で他プロセスに使われている
  ```sh
  lsof -i :8000   # analytics-py
  lsof -i :9000   # processor-go
  lsof -i :3000   # usermgmt-ts
  ```
  該当プロセスを停止するか、`.env` 経由でポートを変更する。
- **前回の残存コンテナ**: `docker compose down` で完全停止 → 再起動。
- **イメージのビルドキャッシュ破損**: `docker compose build --no-cache`
  でリビルド。

## 2. `analytics-py` (Python) のテストが失敗する

### 症状

- `flake8` が E501 や E402 を返す
- `pytest` が `ImportError` / `ModuleNotFoundError` を返す

### 対処

```sh
cd services/analytics-py
pip install -r requirements-dev.txt
flake8 .
pytest -v
```

必ず `.tool-versions` 記載の Python (3.12) を使用してください
(`python --version` で確認)。

## 3. `processor-go` (Go) のテストが失敗する

### 症状

- `go: cannot find main module`
- `go vet` が `undeclared name` を返す

### 対処

`services/processor-go` ディレクトリで実行する必要があります。

```sh
cd services/processor-go
go mod download
go vet ./...
go test -v ./...
```

Go は `.tool-versions` 記載の **1.22 系** を使用してください。

## 4. `usermgmt-ts` (TypeScript) のテストが失敗する

### 症状

- `npm ci` が lockfile mismatch で失敗する
- `npm test` がタイムアウトする
- `npm run lint` が ESLint エラーを返す

### 対処

```sh
cd services/usermgmt-ts
rm -rf node_modules
npm ci
npm run lint
npm test
```

Node.js は `.tool-versions` 記載の **20 系** を使用してください
(`nvm use 20` 等で切り替え)。
`package-lock.json` を手動で編集しないでください
(Dependabot 経由か `npm install <pkg>` で更新)。

## 5. サービス間の名前解決に失敗する

### 症状

コンテナのログに `connection refused` / `no such host` が出る。

### 原因と対処

コンテナ内では `localhost` / `127.0.0.1` ではなく、
`docker-compose.yml` で定義されたサービス名 (`analytics-py`,
`processor-go`, `usermgmt-ts`) で参照する必要があります。

`.env.example` を参考に、以下のように設定してください:

```
ANALYTICS_URL=http://analytics-py:8000
PROCESSOR_URL=http://processor-go:9000
USERMGMT_URL=http://usermgmt-ts:3000
```

ホスト OS から直接アクセスする場合のみ `localhost` を使用します。

## 6. Docker ビルドが極端に遅い / OOM

### 対処

- Docker Desktop / colima のメモリを 4 GB 以上に増やす
- `docker system prune -a` で不要なイメージ / キャッシュを削除
- `docker compose build --parallel` はメモリを多く消費するため、
  低スペック環境では逐次ビルドに切り替える

## 7. CI が緑にならない (PR のブロック)

### チェックリスト

- Python: `flake8 .` の警告ゼロ / `pytest -v` 全通過
- Go: `go vet ./...` / `go test -v ./...` 両方通過
- TypeScript: `npm run lint` / `npm test` 両方通過
- 依存ファイル (`requirements-dev.txt` / `go.mod` / `package.json`)
  を更新した場合は対応するロックファイル / キャッシュキーも同時更新
- `.tool-versions` と CI (`.github/workflows/ci.yml`) の指定バージョンが
  一致しているか

CI 定義は [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
にあります。ローカル実行時は必ず `.tool-versions` に揃えてください。

## 関連ドキュメント

- [architecture.md](./architecture.md) — 3 サービス構成とデータフロー
- [../README.md](../README.md) — セットアップ手順
- [../CONTRIBUTING.md](../CONTRIBUTING.md) — 開発フロー
- [../SECURITY.md](../SECURITY.md) — セキュリティ問題の報告
