include .env

build:
	docker compose build

start: build
	docker compose up --detach

stop:
	docker compose stop producer

clean: stop
	docker compose down --volumes --remove-orphans

open: start
	open http://localhost:${MINIO_CONSOLE_PORT}

logs:
	docker compose logs

duckdb:
	docker compose run --rm duckdb
