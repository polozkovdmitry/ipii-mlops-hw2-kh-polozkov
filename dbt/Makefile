# Запуск из каталога dbt/: make <target>. make help - список целей.
VENV   ?= ../venv
BIN    := $(VENV)/bin
DBT    := $(BIN)/dbt
FLUFF  := $(BIN)/sqlfluff
PYTHON ?= python3.12

CH := docker exec -i clickhouse clickhouse-client

.DEFAULT_GOAL := help
.PHONY: help install up load deps seed run test unit build all full-refresh \
        docs docs-serve lint fix precommit-install precommit clean

help: ## показать цели
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

install: ## создать venv и поставить dbt, sqlfluff, pre-commit
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	$(BIN)/pip install -r requirements.txt

up: ## поднять ClickHouse (и Kafka) из docker-compose.yml
	cd .. && docker compose up -d
	@until $(CH) -q "SELECT 1" >/dev/null 2>&1; do sleep 1; done

load: up ## создать таблицы hw2 и залить data/train.csv напрямую (без Kafka)
	$(CH) --multiquery < ../sql/01_ddl.sql
	$(CH) -q "TRUNCATE TABLE hw2.transactions"
	$(CH) -q "INSERT INTO hw2.transactions SETTINGS date_time_input_format='best_effort' FORMAT CSVWithNames" < ../data/train.csv
	$(CH) -q "SELECT count() AS rows_loaded FROM hw2.transactions"

deps: ## поставить dbt-пакеты
	$(DBT) deps

seed: ## загрузить seeds
	$(DBT) seed

run: ## построить модели
	$(DBT) run

test: ## все тесты (data + singular + unit)
	$(DBT) test

unit: ## только unit tests
	$(DBT) test --select "test_type:unit"

build: ## seed + run + test в порядке DAG
	$(DBT) build

all: deps seed run test ## полный цикл как в задании

full-refresh: ## пересобрать инкрементальную витрину с нуля
	$(DBT) run --full-refresh

docs: ## сгенерировать dbt Docs
	$(DBT) docs generate

docs-serve: docs ## открыть dbt Docs на http://localhost:8080
	$(DBT) docs serve --port 8080

# sqlfluff запускается из корня репозитория, там лежит .sqlfluff (см. комментарий в нём)
lint: ## sqlfluff lint
	cd .. && $(abspath $(FLUFF)) lint dbt/models dbt/tests

fix: ## sqlfluff fix
	cd .. && $(abspath $(FLUFF)) fix dbt/models dbt/tests

precommit-install: ## поставить git-хуки pre-commit
	cd .. && $(abspath $(BIN))/pre-commit install

precommit: ## прогнать pre-commit по всем файлам
	cd .. && $(abspath $(BIN))/pre-commit run --all-files

clean: ## удалить target/, dbt_packages/, logs/
	$(DBT) clean
