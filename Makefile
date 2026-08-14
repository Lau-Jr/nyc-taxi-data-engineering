.PHONY: install ingest test clean

MONTH ?= 2026-01

install:
	pip install -r requirements.txt

ingest:
	python -m src.ingest --month $(MONTH)

test:
	python -m pytest tests/ -v

clean:
	rm -f data/processed/taxi.duckdb
	rm -f logs/ingestion_*.log logs/rejected_*.csv
