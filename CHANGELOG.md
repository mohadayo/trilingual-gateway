# Changelog

このプロジェクトの主な変更点を記録するファイルです。

フォーマットは [Keep a Changelog v1.1.0](https://keepachangelog.com/ja/1.1.0/) に、
バージョン番号は [Semantic Versioning](https://semver.org/lang/ja/) に準拠します。

## [Unreleased]

### Added

- （次回リリースで追加する機能をここに記載）

### Changed

- （挙動の変更をここに記載）

### Deprecated

- （非推奨になった機能をここに記載）

### Removed

- （削除された機能をここに記載）

### Fixed

- （バグ修正をここに記載）

### Security

- （セキュリティ関連の修正をここに記載）

## [0.1.0] - 2026-04-17

初回リリース。Trilingual Gateway の Baseline 実装
（Python analytics / Go real-time processing / TypeScript user management の 3 サービス構成）を記録します。

### Added

- **Python analytics service**: メトリクスの集計・分析 API。
- **Go real-time processing service**: ストリーミング処理と
  リアルタイム集計を担うサービス。
- **TypeScript user management service**: ユーザー管理・認証を担うサービス。
- ローカル開発用の `docker-compose.yml` による 3 サービスの一括起動。
- 共通タスクを集約する `Makefile`。
- リポジトリ運用ドキュメント: `README.md` / `CODE_OF_CONDUCT.md` /
  `SECURITY.md` / `LICENSE` /
  `.github/` 配下の CODEOWNERS / PR テンプレート / Issue テンプレート等。
- 開発補助ファイル: `.gitattributes` / `.gitignore` / `.env.example` /
  `.tool-versions`。
- CI ワークフロー (`.github/workflows/`) による lint / test の自動実行。

[Unreleased]: https://github.com/mohadayo/trilingual-gateway/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mohadayo/trilingual-gateway/releases/tag/v0.1.0
