# Useful Commands for Local Testing (Mac + Docker)

These are the quickest commands to validate the Open Data Platform on a MacBook Air (Apple Silicon) without memorizing the full Docker docs.

## Start / Stop Core Stack
- Start everything (build if needed): `docker compose up -d --build`
- Stop everything (keep data): `docker compose down`
- Stop and wipe volumes (fresh reset): `docker compose down -v`

## Quick Health Checks
- Check containers: `docker compose ps`
- Follow API logs: `docker compose logs -f api`
- Airflow webserver logs: `docker compose logs -f airflow-webserver`
- API liveness probe: `curl http://localhost:8000/health`

## Airflow Ops
- List DAGs: `docker compose exec airflow-scheduler airflow dags list`
- Trigger sample Yahoo pipeline: `docker compose exec airflow-scheduler airflow dags trigger yahoo_finance_daily`
- View DAG run history: `docker compose exec airflow-scheduler airflow dags list-runs -d yahoo_finance_daily`

## DuckDB/Data Inspection
- Show tables: `docker compose exec api python -c "import duckdb; print(duckdb.connect('/app/data/warehouse.duckdb', read_only=True).execute('SHOW TABLES').fetchdf())"`
- Preview stock prices: `docker compose exec api python -c "import duckdb; conn=duckdb.connect('/app/data/warehouse.duckdb', read_only=True); print(conn.execute('SELECT * FROM stock_prices LIMIT 5').fetchdf())"`
- Export to CSV: `docker compose exec api python -c "import duckdb; conn=duckdb.connect('/app/data/warehouse.duckdb', read_only=True); conn.execute(\"COPY (SELECT * FROM stock_prices) TO '/app/data/stock_prices.csv' (HEADER, DELIMITER ',')\")"`

## DBT (inside the API container)
- Run models: `docker compose exec api dbt run --project-dir /app/dbt --profiles-dir /app/dbt`
- Run tests: `docker compose exec api dbt test --project-dir /app/dbt --profiles-dir /app/dbt`

## Semantic/AI Helpers
- Vectorize a table: `curl -X POST "http://localhost:8000/vectorize/your_table" -H "Content-Type: application/json" -d '["text_column"]'`
- Natural-language SQL (UI-driven) still experimental; watch API logs for errors when testing.

## Cleanup
- Remove dangling images: `docker image prune`
- Remove unused volumes (careful): `docker volume prune`
