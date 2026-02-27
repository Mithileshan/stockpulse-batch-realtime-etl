.PHONY: up down down-v build logs ps health lint test test-cov test-unit test-integration

up:
	docker compose up -d --build

down:
	docker compose down

down-v:
	docker compose down -v

build:
	docker compose build

logs:
	docker compose logs -f

ps:
	docker compose ps

health:
	@curl -s http://localhost:8000/health | python3 -m json.tool
	@curl -s http://localhost:8000/ready | python3 -m json.tool

lint:
	ruff check services/ --select E,F,W --ignore E501

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=services --cov-report=html --cov-report=term

test-unit:
	pytest tests/ -v -m "not slow"

test-integration:
	pytest tests/ -v -m "integration"
