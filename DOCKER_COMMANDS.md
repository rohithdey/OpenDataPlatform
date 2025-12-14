# Docker Commands Reference for OpenDataPlatform

## Quick Reference

| Service | Port | URL |
|---------|------|-----|
| UI | 3000 | http://localhost:3000 |
| API | 8000 | http://localhost:8000 |
| Airflow | 8080 | http://localhost:8080 (admin/admin) |
| DuckDB IDE | 8978 | http://localhost:8978 |
| Ollama | 11434 | http://localhost:11434 |

---

## Starting & Stopping

```bash
# Start all services
docker-compose up -d

# Start all services and rebuild images
docker-compose up -d --build

# Start specific service(s)
docker-compose up -d api ui airflow-webserver

# Stop all services (keeps data)
docker-compose down

# Stop and remove volumes (DELETES DATA!)
docker-compose down -v

# Restart a specific service
docker-compose restart api

# Restart all services
docker-compose restart
```

---

## Building & Rebuilding

```bash
# Rebuild all images (after Dockerfile changes)
docker-compose build

# Rebuild without cache (fresh build)
docker-compose build --no-cache

# Rebuild specific service
docker-compose build api

# Rebuild and start
docker-compose up -d --build
```

---

## Viewing Logs

```bash
# View all logs
docker-compose logs

# View logs for specific service
docker-compose logs api
docker-compose logs airflow-scheduler

# Follow logs in real-time
docker-compose logs -f

# Follow logs for specific service
docker-compose logs -f api

# Last 100 lines of logs
docker-compose logs --tail=100 api

# Logs since timestamp
docker-compose logs --since="2024-01-15T10:00:00" api
```

---

## Executing Commands in Containers

```bash
# Open bash shell in container
docker-compose exec api bash
docker-compose exec airflow-scheduler bash

# Run single command
docker-compose exec api python --version
docker-compose exec airflow-scheduler airflow dags list

# Run as root user
docker-compose exec -u root api bash

# Run DuckDB CLI directly
docker-compose exec api python -c "import duckdb; conn = duckdb.connect('/app/data/warehouse.duckdb'); print(conn.execute('SELECT * FROM stock_prices LIMIT 5').fetchdf())"
```

---

## Airflow Commands

```bash
# List all DAGs
docker-compose exec airflow-scheduler airflow dags list

# Trigger a DAG
docker-compose exec airflow-scheduler airflow dags trigger yahoo_finance_daily

# Trigger DAG with config
docker-compose exec airflow-scheduler airflow dags trigger yahoo_finance_daily --conf '{"symbols": "AAPL,TSLA"}'

# Unpause a DAG
docker-compose exec airflow-scheduler airflow dags unpause yahoo_finance_daily

# Pause a DAG
docker-compose exec airflow-scheduler airflow dags pause yahoo_finance_daily

# List DAG runs
docker-compose exec airflow-scheduler airflow dags list-runs -d yahoo_finance_daily

# Check DAG status
docker-compose exec airflow-scheduler airflow dags state yahoo_finance_daily 2024-01-15
```

---

## DBT Commands

```bash
# Run all DBT models
docker-compose exec airflow-scheduler dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# Run specific model
docker-compose exec airflow-scheduler dbt run --select mart_stock_summary --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# Run with full refresh
docker-compose exec airflow-scheduler dbt run --full-refresh --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# Run DBT tests
docker-compose exec airflow-scheduler dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# Generate DBT docs
docker-compose exec airflow-scheduler dbt docs generate --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# Show DBT dependencies
docker-compose exec airflow-scheduler dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

---

## DuckDB Queries

```bash
# Quick query from host
docker-compose exec api python -c "
import duckdb
conn = duckdb.connect('/app/data/warehouse.duckdb', read_only=True)
print(conn.execute('SHOW TABLES').fetchdf())
"

# Interactive DuckDB session
docker-compose exec api python -c "
import duckdb
conn = duckdb.connect('/app/data/warehouse.duckdb', read_only=True)
print(conn.execute('SELECT symbol, COUNT(*) as rows FROM stock_prices GROUP BY symbol').fetchdf())
"

# Export query to CSV
docker-compose exec api python -c "
import duckdb
conn = duckdb.connect('/app/data/warehouse.duckdb', read_only=True)
conn.execute(\"COPY (SELECT * FROM stock_prices) TO '/app/data/export.csv' (HEADER, DELIMITER ',')\")
"
```

---

## Container Management

```bash
# List running containers
docker-compose ps

# List all containers (including stopped)
docker-compose ps -a

# Check container resource usage
docker stats

# Inspect a container
docker-compose exec api cat /etc/os-release

# View container environment variables
docker-compose exec api env

# Check disk usage
docker system df
```

---

## Data & Volumes

```bash
# List volumes
docker volume ls

# Inspect a volume
docker volume inspect opendataplatform_postgres-db-volume

# Backup DuckDB database
cp ./data/warehouse.duckdb ./data/warehouse.duckdb.backup

# View data directory
ls -la ./data/

# View Delta Lake tables
ls -la ./data/delta/
```

---

## Cleanup Commands

```bash
# Remove stopped containers
docker-compose rm

# Remove unused images
docker image prune

# Remove unused volumes (CAREFUL!)
docker volume prune

# Remove all unused resources
docker system prune

# Nuclear option - remove everything (DANGEROUS!)
docker system prune -a --volumes
```

---

## Troubleshooting

```bash
# Check if services are healthy
docker-compose ps

# View service configuration
docker-compose config

# Check container logs for errors
docker-compose logs api 2>&1 | grep -i error

# Check Airflow scheduler logs
docker-compose logs airflow-scheduler 2>&1 | tail -50

# Test API endpoint
curl http://localhost:8000/health

# Test Airflow API
curl -u admin:admin http://localhost:8080/api/v1/dags

# Check network connectivity
docker-compose exec api ping postgres

# Restart specific service if stuck
docker-compose restart airflow-scheduler

# Force recreate containers
docker-compose up -d --force-recreate
```

---

## Common Workflows

### Fresh Start (Reset Everything)
```bash
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
# Wait for services to initialize
sleep 30
# Check status
docker-compose ps
```

### Update Code and Restart
```bash
git pull
docker-compose build
docker-compose up -d
```

### Debug a Failed DAG
```bash
# Check scheduler logs
docker-compose logs -f airflow-scheduler

# Check specific task logs
docker-compose exec airflow-scheduler cat /opt/airflow/logs/dag_id=yahoo_finance_daily/run_id=*/task_id=fetch_yahoo_data/*.log
```

### Export Data for Analysis
```bash
# Export to CSV
docker-compose exec api python -c "
import duckdb
import pandas as pd
conn = duckdb.connect('/app/data/warehouse.duckdb', read_only=True)
df = conn.execute('SELECT * FROM stock_prices').fetchdf()
df.to_csv('/app/data/stock_prices_export.csv', index=False)
print(f'Exported {len(df)} rows to /app/data/stock_prices_export.csv')
"

# File is now at ./data/stock_prices_export.csv on your host
```

---

## Port Mapping Reference

If you need to change ports (e.g., conflicts), edit `docker-compose.yml`:

```yaml
ports:
  - "HOST_PORT:CONTAINER_PORT"
```

| Service | Default | Change to |
|---------|---------|-----------|
| UI | 3000:3000 | 3001:3000 |
| API | 8000:8000 | 8001:8000 |
| Airflow | 8080:8080 | 8081:8080 |
| DuckDB IDE | 8978:8978 | 8979:8978 |
| Ollama | 11434:11434 | 11435:11434 |

---

## Environment Variables

Create a `.env` file in the project root:

```bash
# .env
AIRFLOW_UID=50000
OPENAI_API_KEY=sk-your-key-here
FRED_API_KEY=your-fred-key-here
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=your-secure-password
```
