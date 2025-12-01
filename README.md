# Open Data Platform

A portable, containerized data platform combining Apache Airflow, DuckDB, Apache Iceberg, DBT, and semantic search capabilities.

## Features

- **Apache Airflow** - Orchestrate and schedule data pipelines
- **DuckDB** - Fast analytical database for your data warehouse
- **Apache Iceberg** - Table format for large analytic datasets
- **DBT** - Transform your data with SQL
- **Semantic Search** - Query your data in plain English
- **Modern UI** - Web interface for managing everything

## Quick Start

1. Make sure Docker is installed and running
2. Clone this repository
3. Run the startup script:

```bash
chmod +x start.sh
./start.sh
```

4. Access the services:
   - **UI Dashboard**: http://localhost:3000
   - **Airflow**: http://localhost:8080 (admin/admin)
   - **API**: http://localhost:8000
   - **API Docs**: http://localhost:8000/docs

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        UI (React)                           │
│                     localhost:3000                          │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    API (FastAPI)                            │
│                     localhost:8000                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   DuckDB    │  │  Semantic   │  │   Airflow API      │  │
│  │   Queries   │  │   Search    │  │   Integration      │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                Apache Airflow                               │
│                localhost:8080                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Scheduler  │  │  Webserver  │  │     Triggerer       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    Data Layer                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              DuckDB + Iceberg                        │   │
│  │              (warehouse.duckdb)                      │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Using the Platform

### 1. Data Explorer
- View all tables in your DuckDB database
- Preview data from any table
- Upload CSV, JSON, or Parquet files

### 2. SQL Editor
- Write and execute SQL queries
- View results in a table format
- Supports all DuckDB SQL features

### 3. Jobs (Airflow Pipelines)
- View all DAGs
- Trigger DAG runs
- Create new pipelines through the UI
- Monitor run status

### 4. Ask Data (Semantic Search)
- Vectorize tables for semantic search
- Ask questions in plain English
- Get answers with source data references

## Sample DAG: GLEIF Data

The platform includes a sample DAG that pulls Legal Entity Identifier (LEI) data from GLEIF:

1. Go to Airflow (http://localhost:8080)
2. Enable the `gleif_data_ingestion` DAG
3. Trigger a run
4. View the data in the UI's Data Explorer

## Adding Your Own Data

### Via File Upload
1. Go to Data Explorer
2. Enter a table name
3. Upload a CSV, JSON, or Parquet file

### Via Custom DAG
1. Go to Jobs
2. Click "New Pipeline"
3. Configure your data source
4. Set the schedule

### Via SQL
```sql
CREATE TABLE my_table AS 
SELECT * FROM read_csv_auto('/path/to/file.csv');
```

## Semantic Search Setup

To enable natural language queries:

1. Go to "Ask Data" tab
2. Select a table to vectorize
3. Choose which text columns to index
4. Click "Vectorize Table"
5. Start asking questions!

### Optional: OpenAI Integration

For better natural language answers, add your OpenAI API key:

```bash
# Edit .env file
OPENAI_API_KEY=your-key-here
```

Then restart the platform.

## DBT Transformations

DBT models are in the `dbt/models/` directory. To run transformations:

```bash
# Enter the API container
docker compose exec api bash

# Run DBT
cd /app/dbt
dbt run
```

## Deploying to AWS

### Using EC2

1. Launch an EC2 instance (t3.medium or larger recommended)
2. Install Docker and Docker Compose
3. Clone this repository
4. Run `./start.sh`
5. Configure security groups to allow ports 3000, 8000, 8080

### Using ECS

1. Build and push images to ECR
2. Create ECS task definitions
3. Set up an Application Load Balancer
4. Configure ECS services

## Sharing with Collaborators

### Option 1: Share the Code
1. Push to GitHub
2. Collaborators clone and run `./start.sh`

### Option 2: Host on a Server
1. Deploy to a cloud server
2. Share the server URL
3. Collaborators access via browser

## Stopping the Platform

```bash
./stop.sh

# Or to remove all data:
docker compose down -v
```

## Troubleshooting

### Services won't start
```bash
# Check logs
docker compose logs

# Check specific service
docker compose logs api
docker compose logs airflow-webserver
```

### Port already in use
```bash
# Find what's using the port
lsof -i :8080

# Kill the process or change ports in docker-compose.yml
```

### Permission issues on Linux
```bash
# Set correct Airflow UID
echo "AIRFLOW_UID=$(id -u)" >> .env
```

## License

MIT
