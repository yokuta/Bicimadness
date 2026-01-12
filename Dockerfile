FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DUCKDB_PATH=/app/data/bicimad.duckdb
ENV PORT=8000

# 1) Descarga el DuckDB desde R2 (si no existe)
# 2) Arranca la API
CMD ["sh", "-c", "python download_db.py && uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
