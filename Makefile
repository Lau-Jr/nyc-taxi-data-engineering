.PHONY: install ingest test dashboard features baseline profile clean

MONTH ?= 2026-01
LABEL ?= latest

install:
	pip install -r requirements.txt

ingest:
	python -m src.ingest --month $(MONTH)

dashboard:
	streamlit run streamlit_app.py

features:
	python -m src.features

baseline:
	python -m src.baseline

profile:
	python -m src.profile_pipeline --month $(MONTH) --label $(LABEL)

test:
	python -m pytest tests/ -v

clean:
	rm -f data/processed/taxi.duckdb
	rm -f logs/ingestion_*.log logs/rejected_*.csv
