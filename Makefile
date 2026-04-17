.PHONY: test test-python test-go test-ts up down build lint

test: test-python test-go test-ts
	@echo "All tests passed."

test-python:
	cd services/analytics-py && pip install -q -r requirements.txt && pytest -v

test-go:
	cd services/processor-go && go test -v ./...

test-ts:
	cd services/usermgmt-ts && npm install && npm test

lint:
	cd services/analytics-py && pip install -q flake8 && flake8 --max-line-length=120 app.py test_app.py
	cd services/processor-go && go vet ./...
	cd services/usermgmt-ts && npm install && npx eslint src/

up:
	docker compose up -d --build

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f

ps:
	docker compose ps
