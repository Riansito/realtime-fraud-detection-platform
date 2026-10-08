.PHONY: setup lint format

setup:
	pip install pre-commit ruff
	pre-commit install

lint:
	ruff check .

format:
	ruff format .
