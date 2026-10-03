.PHONY: install ingest test dashboard clean

MONTH ?= 2026-01

install:
	pip install -r requirements.txt

ingest:
	python -m src.ingest --month $(MONTH)

dashboard:
	streamlit run streamlit_app.py

test:
	python -m pytest tests/ -v

clean:
	rm -f data/processed/taxi.duckdb
	rm -f logs/ingestion_*.log logs/rejected_*.csv
