.PHONY: up up-db down logs infisical-up ingest gateway test

up:
	infisical run --env dev --path /github-app --domain http://localhost:8080 -- docker compose up -d --build

up-db:
	docker compose up -d postgres redis

down:
	docker compose down

logs:
	docker compose logs -f

infisical-up:
	docker compose -f infra/infisical/docker-compose.yml --env-file infra/infisical/.env up -d

ingest:
	cd services/ingest && infisical run --env dev --path /github-app --domain http://localhost:8080 -- cargo run

gateway:
	cd services/gateway && infisical run --env dev --path /github-app --domain http://localhost:8080 -- mise exec -- mix run --no-halt

test:
	cd services/ingest && cargo test
	cd services/gateway && mise exec -- mix test
