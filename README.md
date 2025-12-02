# 🚀 OpenDataPlatform

Open-source data platform with AI-powered natural language queries. Built for SMBs who want data analytics without the complexity or costs of enterprise solutions.

## ⚡ Key Features

- 🤖 **AI-Powered Queries**: Ask questions in plain English, get SQL + results (zero AI API costs!)
- 📊 **SQL Editor with Pagination**: Execute queries without browser crashes on large datasets
- 🆓 **Local AI (Ollama)**: SQLCoder + Llama 3.2 for text-to-SQL and natural language answers
- 🔒 **Complete Privacy**: All AI runs locally or on-prem (no data leaves your infrastructure)
- 📈 **Apache Airflow**: Orchestrate and schedule ETL pipelines
- 🗄️ **DuckDB**: Fast analytical database perfect for data warehouses
- 💰 **Flat-Rate SaaS Ready**: Predictable costs, no per-query charges
- **DBT Integration**: Transform your data with SQL
- **Modern Dark UI**: Clean, professional web interface

## 🎯 Quick Start

### For macOS (M1/M2/M3/M4) - **Recommended for Development**

**5-10x faster with Metal GPU acceleration!**

```bash
# 1. Install native Ollama + AI models (~5-10 min one-time setup)
chmod +x setup-ollama-mac.sh
./setup-ollama-mac.sh

# 2. Start all services with native Ollama
docker-compose -f docker-compose.yml -f docker-compose.mac.yml up -d --build
```

**Performance**: 1-6 seconds per AI query, 20-40% CPU usage

---

### For Linux/Windows or Standard Docker

```bash
# 1. Start all services
docker-compose up -d --build

# 2. Pull AI models (one-time, ~5-10 minutes)
chmod +x setup-ollama.sh
./setup-ollama.sh
```

**Performance**: 10-60 seconds per AI query (CPU only)

---

### Access the Platform

- **UI Dashboard**: http://localhost:3000
- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Airflow**: http://localhost:8081 (admin/admin)

---

### For GPU-Enabled Deployments (Cloud/Enterprise)

See **[DEPLOYMENT-STRATEGY.md](DEPLOYMENT-STRATEGY.md)** for:
- NVIDIA GPU setup (0.5-3s per query!)
- Cloud deployment options (AWS, GCP, Azure)
- Hardware recommendations
- Cost analysis

## 💰 Why Local AI Matters for Your Business

| Solution | Cost per Query | 100k queries/month | Business Model |
|----------|----------------|-------------------|----------------|
| **Ollama (Local)** | **$0** | **$0** | ✅ Flat-rate pricing |
| OpenAI GPT-4 | $0.10-0.50 | $10,000-50,000 | ❌ Per-query pricing |
| Anthropic Claude | $0.08-0.40 | $8,000-40,000 | ❌ Per-query pricing |

**Result**: With local AI, you can offer flat-rate SaaS pricing ($99-999/month) with predictable costs and healthy margins!

## 🤖 AI Models (Dual-Model Approach)

We use **two specialized models** for optimal results:

1. **SQLCoder (7B)** - Generates accurate SQL queries from natural language
2. **Llama 3.2 (3B)** - Creates conversational natural language answers

### Why Two Models?
- SQLCoder: Excellent at SQL, poor at conversation
- Llama 3.2: Excellent at conversation, less specialized for SQL
- **Together**: Best of both worlds!

### Model Sizes
- Total: ~6GB disk space (SQLCoder 4GB + Llama 3.2 2GB)
- One-time download, then instant loading

## 🏗️ Architecture

```
┌──────────────┐
│  React UI    │  ← User Interface (port 3000)
│ (Dark Theme) │     • SQL Editor with pagination
└──────┬───────┘     • Natural language queries
       │             • Data explorer
       ↓
┌──────────────┐
│  FastAPI     │  ← Backend API (port 8000)
│  (Python)    │     • SQL execution
└──────┬───────┘     • AI orchestration
       │             • Airflow integration
       │
   ┌───┴────┬────────────────┐
   ↓        ↓                ↓
┌──────┐ ┌─────────┐   ┌─────────┐
│DuckDB│ │ Ollama  │   │Airflow  │
│      │ │SQLCoder │   │Scheduler│
│      │ │Llama3.2 │   │Webserver│
└──────┘ └─────────┘   └─────────┘
Warehouse   Local AI      ETL
(port N/A)  (port11434)  (port 8081)
```

**Data Flow for AI Queries**:
1. User asks: "Show me all customers from California"
2. SQLCoder generates: `SELECT * FROM customers WHERE state = 'CA' LIMIT 100`
3. DuckDB executes query
4. Llama 3.2 creates answer: "Found 156 customers from California. Here are the first 100..."

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

### 4. Ask Data (AI-Powered Queries)
- Ask questions in plain English - **no SQL knowledge required!**
- AI generates SQL automatically using SQLCoder
- Get natural language answers using Llama 3.2
- See the generated SQL query and results
- Green badge = Local Ollama (fast, free) | Purple badge = OpenAI (fallback)

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

## 📚 Documentation

- **[SETUP-MAC.md](SETUP-MAC.md)** - macOS setup guide (M1/M2/M3/M4 with 5-10x speedup)
- **[DEPLOYMENT.md](DEPLOYMENT.md)** - Standard deployment guide (Linux/Windows)
- **[DEPLOYMENT-STRATEGY.md](DEPLOYMENT-STRATEGY.md)** - **Complete business guide**:
  - When to use Mac native vs Docker vs GPU
  - Hardware recommendations for each scenario
  - Cost analysis (development, clients, cloud)
  - Pricing tier suggestions
  - Deployment packages for clients
  - Performance benchmarks

## 🔄 Flexible Deployment (Switch Anytime!)

Your setup supports **all deployment modes** with simple compose file changes:

```bash
# Development on Mac (fastest for you - 1-6s queries)
docker-compose -f docker-compose.yml -f docker-compose.mac.yml up -d

# Client on-prem or cloud CPU (10-60s queries)
docker-compose -f docker-compose.yml up -d

# Cloud or on-prem with NVIDIA GPU (0.5-3s queries)
docker-compose -f docker-compose.yml -f docker-compose.gpu.yml up -d
```

### Optional: OpenAI Fallback

Ollama runs locally by default (free, private). For fallback to OpenAI:

```bash
# Add to .env file (optional)
OPENAI_API_KEY=your-key-here
```

Platform will automatically:
1. Try Ollama first (local, free)
2. Fall back to OpenAI if Ollama unavailable
3. Show which AI was used with colored badge

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
