# 運用ランブック (Runbook)

Trilingual Gateway（Python / Go / TypeScript の 3 言語構成）の運用中に発生し得るインシデントへの初動対応をまとめたランブックです。
症状ベースの逆引きは [`docs/TROUBLESHOOTING.md`](./TROUBLESHOOTING.md) を、システム全体像は [`docs/architecture.md`](./architecture.md) を、よくある質問は [`docs/FAQ.md`](./FAQ.md) を参照してください。

## 1. インシデント初動フロー

1. **一次受信** — アラート内容 / 発生時刻 / 影響サービスを記録
2. **影響範囲の確認**
   - `usermgmt-ts` (TypeScript ゲートウェイ) のヘルスチェック応答
   - `processor-go` (Go プロセッサ) の応答
   - `analytics-py` (Python 集計) の応答
   - `docker-compose ps` で全サービスの状態
3. **暫定対応** — 影響が拡大している場合は該当サービスをリスタート（下記 §2）
4. **原因調査** — [`docs/TROUBLESHOOTING.md`](./TROUBLESHOOTING.md) の症状マトリクスから該当項目を辿る
5. **恒久対応** — 修正 PR / 設定変更 / 依存側への連絡
6. **事後まとめ** — 影響時間・原因・再発防止策を Issue に記録

## 2. サービス別リカバリ手順

3 言語それぞれのランタイム特有の観点をまとめます。

### 2.1 usermgmt-ts (TypeScript / Node.js)

- **再起動:** `docker-compose restart usermgmt-ts`
- **ログ確認:** `docker-compose logs --tail=200 -f usermgmt-ts`
- **依存の再取得（`node_modules` 破損時）:**
  ```bash
  docker-compose exec usermgmt-ts sh -c "rm -rf node_modules && npm ci"
  ```
- **セキュリティヘッダの確認:** レスポンスヘッダに `X-Powered-By` が **含まれていない** ことを確認（PR #165）

### 2.2 processor-go (Go)

- **再起動:** `docker-compose restart processor-go`
- **ログ確認:** `docker-compose logs --tail=200 -f processor-go`
- **バイナリの入れ替え:** バグ修正後は `docker-compose up -d --build processor-go` で再ビルド後デプロイ
- **時系列集計 (`by_week` / `by_month`) の異常:** PR #151 の追加分。パラメータ境界値と結果件数を先に確認

### 2.3 analytics-py (Python)

- **再起動:** `docker-compose restart analytics-py`
- **ログ確認:** `docker-compose logs --tail=200 -f analytics-py`
- **依存の再取得（`site-packages` 破損時）:**
  ```bash
  docker-compose exec analytics-py sh -c "pip install --no-cache-dir -r requirements.txt"
  ```
- **メモリ問題:** 集計ジョブが OOM する場合はバッチサイズや対象範囲の見直しを検討

## 3. 共通の運用コマンド集

```bash
# 全サービスの状態
docker-compose ps

# 全サービスのログを直近 200 行 + 追従
docker-compose logs --tail=200 -f

# 特定サービスのみ再起動
docker-compose restart <service>

# 特定サービスのみ再ビルドして再起動
docker-compose up -d --build <service>

# コンテナ内シェル
docker-compose exec <service> sh

# ボリューム / ネットワーク状態
docker volume ls
docker network ls
```

## 4. エスカレーション基準

以下のいずれかに該当した場合、オンコール担当を追加でエスカレーションします。

| 条件                                       | 対応レベル |
| ------------------------------------------ | ---------- |
| ユーザ影響のある 5xx が 5 分以上継続       | Sev-1      |
| 単一言語ランタイムのクラッシュループ       | Sev-1      |
| 単一サービスの機能限定的障害               | Sev-2      |
| 集計 (analytics-py) の遅延 / 部分欠損       | Sev-2      |
| 監視系の誤検知 / ラウンドトリップ増加       | Sev-3      |
| セキュリティに関する疑いのある事象         | 即エスカレーション ([`SECURITY.md`](../SECURITY.md) 参照) |

## 5. 関連ドキュメント

- [`docs/architecture.md`](./architecture.md) — システム全体像と 3 言語の責務境界
- [`docs/TROUBLESHOOTING.md`](./TROUBLESHOOTING.md) — 症状 → 原因 → 対処
- [`docs/FAQ.md`](./FAQ.md) — よくある質問
- [`CONTRIBUTING.md`](../CONTRIBUTING.md) — 変更 PR 手順
- [`SECURITY.md`](../SECURITY.md) — 脆弱性報告と対応

## 6. 更新方針

- 新しいアラート・失敗パターンが判明した場合は本ファイルに恒久項目として追加する
- 一時的な対処ノウハウは `TROUBLESHOOTING.md` に、フロー / 手順として確立したものを本ファイルに集約する
