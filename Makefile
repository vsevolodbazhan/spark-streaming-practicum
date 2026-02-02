include .env

build:
	docker compose build

start: build
	docker compose up --detach

stop:
	docker compose stop producer

clean: stop
	docker compose down --volumes --remove-orphans --rmi local

producer-logs:
	docker compose logs producer

consumer-logs:
	docker compose logs consumer

minio: start
	open http://localhost:${MINIO_CONSOLE_PORT}

duckdb:
	docker compose run --rm duckdb

spark:
	open http://localhost:${CONSUMER_SPARK_UI_PORT}
