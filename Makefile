.PHONY: help test test-python test-go test-ts up down build lint logs ps clean

# `make` を引数なしで叩いた場合はターゲット一覧を表示する。
.DEFAULT_GOAL := help

help: ## 利用可能な make ターゲットの一覧を表示
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: test-python test-go test-ts ## すべてのサービスのテストを実行
	@echo "All tests passed."

test-python: ## Python サービス (analytics-py) のテストを実行
	cd services/analytics-py && pip install -q -r requirements-dev.txt && pytest -v

test-go: ## Go サービス (processor-go) のテストを実行 (race detector 有効)
	cd services/processor-go && go test -race -v ./...

test-ts: ## TypeScript サービス (usermgmt-ts) のテストを実行
	cd services/usermgmt-ts && npm ci && npm test

lint: ## すべてのサービスに対して lint を実行
	cd services/analytics-py && pip install -q -r requirements-dev.txt && flake8 --max-line-length=120 --exclude=__pycache__ .
	cd services/processor-go && go vet ./...
	cd services/usermgmt-ts && npm ci && npx eslint src/

up: ## docker compose でスタックをバックグラウンド起動
	docker compose up -d --build

down: ## docker compose スタックを停止
	docker compose down

build: ## docker compose イメージをビルド
	docker compose build

logs: ## docker compose のログをフォロー
	docker compose logs -f

ps: ## docker compose サービスの状態一覧
	docker compose ps

clean: ## テスト・ビルド成果物とキャッシュを削除
	find services/analytics-py -type d -name __pycache__ -prune -exec rm -rf {} +
	find services/analytics-py -type d -name .pytest_cache -prune -exec rm -rf {} +
	rm -rf services/usermgmt-ts/node_modules services/usermgmt-ts/dist services/usermgmt-ts/coverage
