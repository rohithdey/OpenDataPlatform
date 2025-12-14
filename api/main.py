"""
Data Platform API
Backend service for the Open Data Platform
"""

import os
import json
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile, File, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import requests

# Initialize FastAPI app
app = FastAPI(
    title="Open Data Platform API",
    description="API for managing data pipelines, DuckDB queries, and semantic search",
    version="1.0.0"
)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
# NOTE: These defaults are convenient for local demos but should be overridden via
# environment variables in production deployments (e.g., non-admin Airflow creds,
# SSL-enabled endpoints, and isolated DuckDB paths per environment).
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse.duckdb")
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Ensure data directory exists
os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

# Models
class SQLQuery(BaseModel):
    query: str

class SemanticQuery(BaseModel):
    question: str
    table_name: Optional[str] = None

class DAGConfig(BaseModel):
    dag_id: str
    description: str
    schedule: str
    source_type: str  # 'api', 'file', 'database'
    source_config: Dict[str, Any]
    target_table: str

class TableInfo(BaseModel):
    name: str
    columns: List[Dict[str, str]]
    row_count: int

# Database connection helper
def get_db_connection(read_only=True):
    """Get DuckDB connection - uses read_only by default to avoid locks"""
    conn = duckdb.connect(DUCKDB_PATH, read_only=read_only)
    # Note: Iceberg extension removed - not needed and causes issues on ARM Macs
    # TODO: wrap in a context manager to ensure the connection is always closed
    # when exceptions are raised in callers.
    return conn

# Vector store for semantic search
class VectorStore:
    def __init__(self):
        self.embeddings = {}
        self.model = None
    
    def load_model(self):
        if self.model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self.model = SentenceTransformer('all-MiniLM-L6-v2')
            except Exception as e:
                print(f"Could not load embedding model: {e}")
                return False
        return True
    
    def embed_text(self, text: str) -> List[float]:
        if not self.load_model():
            return []
        return self.model.encode(text).tolist()
    
    def vectorize_table(self, table_name: str, text_columns: List[str]):
        """Vectorize text columns in a table"""
        if not self.load_model():
            return {"error": "Could not load embedding model"}
        
        conn = get_db_connection()
        
        # Get data from table
        query = f"SELECT * FROM {table_name}"
        df = conn.execute(query).fetchdf()

        # Create combined text from specified columns
        df['_combined_text'] = df[text_columns].astype(str).agg(' | '.join, axis=1)
        
        # Generate embeddings
        embeddings = self.model.encode(df['_combined_text'].tolist())
        
        # Store embeddings with row indices - convert to JSON-safe format for timestamps
        self.embeddings[table_name] = {
            'vectors': embeddings,
            'data': json.loads(df.to_json(orient='records', date_format='iso')),
            'text_columns': text_columns
        }

        # REVIEW: This is kept in memory only. For production durability and
        # multi-worker scaling, consider persisting embeddings to disk or a
        # vector database (e.g., pgvector, LanceDB, Qdrant) instead of storing
        # them on the process object.

        conn.close()
        return {"status": "success", "rows_vectorized": len(df)}
    
    def semantic_search(self, query: str, table_name: str, top_k: int = 5):
        """Search for semantically similar rows"""
        if table_name not in self.embeddings:
            return {"error": f"Table {table_name} not vectorized"}
        
        if not self.load_model():
            return {"error": "Could not load embedding model"}
        
        query_embedding = self.model.encode(query)
        stored = self.embeddings[table_name]
        
        # Calculate cosine similarities
        similarities = np.dot(stored['vectors'], query_embedding) / (
            np.linalg.norm(stored['vectors'], axis=1) * np.linalg.norm(query_embedding)
        )
        
        # Get top k results
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
        results = []
        for idx in top_indices:
            results.append({
                'data': stored['data'][idx],
                'similarity': float(similarities[idx])
            })
        
        return results

vector_store = VectorStore()

# API Endpoints

@app.get("/")
async def root():
    return {"message": "Open Data Platform API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# ============== DuckDB Endpoints ==============

@app.get("/tables")
async def list_tables():
    """List all tables in the DuckDB database"""
    try:
        conn = get_db_connection()
        result = conn.execute("SHOW TABLES").fetchall()
        tables = [row[0] for row in result]
        conn.close()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{table_name}")
async def get_table_info(table_name: str):
    """Get information about a specific table"""
    try:
        conn = get_db_connection()
        
        # Get column info
        columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
        column_info = [{"name": col[0], "type": col[1]} for col in columns]
        
        # Get row count
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        conn.close()
        return TableInfo(name=table_name, columns=column_info, row_count=count)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{table_name}/preview")
async def preview_table(table_name: str, limit: int = 100):
    """Preview data from a table"""
    try:
        conn = get_db_connection()
        df = conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchdf()
        conn.close()
        return {"data": df.to_dict('records'), "columns": list(df.columns)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def execute_query(query_request: SQLQuery):
    """Execute a SQL query against DuckDB"""
    try:
        # Check if it's a SELECT query
        query = query_request.query.strip()
        is_select = query.upper().startswith("SELECT") or query.upper().startswith("WITH") or query.upper().startswith("SHOW") or query.upper().startswith("DESCRIBE")

        # REVIEW: No SQL injection protection is added here because the API
        # expects raw SQL. For a multi-tenant deployment, consider adding an
        # allowlist of statements or a sandboxed parser to avoid destructive
        # operations (e.g., DROP TABLE) when exposed to untrusted users.

        # Use read_only for SELECT, write access for modifications
        conn = get_db_connection(read_only=is_select)
        
        if is_select:
            df = conn.execute(query).fetchdf()
            result = {
                "data": df.to_dict('records'),
                "columns": list(df.columns),
                "row_count": len(df)
            }
        else:
            conn.execute(query)
            result = {"message": "Query executed successfully"}
        
        conn.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    table_name: str = Query(..., description="Target table name")
):
    """Upload a CSV/JSON file and load into DuckDB"""
    try:
        # Save file temporarily
        temp_path = f"/tmp/{file.filename}"
        with open(temp_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        conn = get_db_connection(read_only=False)
        
        # Determine file type and load
        if file.filename.endswith('.csv'):
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv_auto('{temp_path}')")
        elif file.filename.endswith('.json'):
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_json_auto('{temp_path}')")
        elif file.filename.endswith('.parquet'):
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_parquet('{temp_path}')")
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")

        # REVIEW: Table names are not validated or quoted, so user input could
        # collide with existing schema or create awkward identifiers. Consider
        # enforcing a safe naming convention (alphanumeric + underscore) before
        # creating tables from uploads.

        # Get row count
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        conn.close()
        
        # Clean up temp file
        os.remove(temp_path)
        
        return {"message": f"Data loaded into table {table_name}", "rows_loaded": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============== Semantic Search Endpoints ==============

@app.post("/vectorize/{table_name}")
async def vectorize_table(
    table_name: str,
    text_columns: List[str] = Body(..., description="Columns to use for text embedding")
):
    """Vectorize a table for semantic search"""
    try:
        result = vector_store.vectorize_table(table_name, text_columns)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/semantic-search")
async def semantic_search(query: SemanticQuery):
    """Perform semantic search on vectorized data"""
    try:
        if query.table_name:
            results = vector_store.semantic_search(query.question, query.table_name)
            return {"results": results}
        else:
            # Search across all vectorized tables
            all_results = {}
            for table_name in vector_store.embeddings.keys():
                results = vector_store.semantic_search(query.question, table_name)
                all_results[table_name] = results
            return {"results": all_results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ask")
async def ask_question(query: SemanticQuery):
    """Ask a natural language question about the data"""
    try:
        # First, get relevant data through semantic search
        if query.table_name:
            search_results = vector_store.semantic_search(query.question, query.table_name, top_k=10)
        else:
            search_results = []
            for table_name in vector_store.embeddings.keys():
                results = vector_store.semantic_search(query.question, table_name, top_k=5)
                search_results.extend(results)
        
        if not search_results:
            return {"answer": "No relevant data found. Please vectorize your tables first."}
        
        # Format context for LLM
        context = "Based on the following data:\n\n"
        for i, result in enumerate(search_results[:5]):
            context += f"Record {i+1}: {json.dumps(result['data'], indent=2)}\n\n"
        
        # If OpenAI API key is available, use it for natural language response
        if OPENAI_API_KEY:
            try:
                import openai
                client = openai.OpenAI(api_key=OPENAI_API_KEY)
                
                response = client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a helpful data assistant. Answer questions based on the provided data context. Be concise and accurate."},
                        {"role": "user", "content": f"{context}\n\nQuestion: {query.question}"}
                    ],
                    max_tokens=500
                )
                
                return {
                    "answer": response.choices[0].message.content,
                    "source_data": search_results[:5]
                }
            except Exception as e:
                print(f"OpenAI error: {e}")

        # REVIEW: No streaming or token-usage guards are applied; a malicious
        # prompt could incur unexpected OpenAI costs. Add rate limiting and a
        # max_tokens clamp per user/session if this endpoint becomes public.

        # Fallback: return the most relevant data
        return {
            "answer": f"Here are the most relevant records for your question:",
            "source_data": search_results[:5]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============== Airflow Integration Endpoints ==============

@app.get("/dags")
async def list_dags():
    """List all DAGs from Airflow"""
    try:
        response = requests.get(
            f"{AIRFLOW_API_URL}/dags",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Airflow API error: {str(e)}")

@app.get("/dags/{dag_id}")
async def get_dag(dag_id: str):
    """Get details of a specific DAG"""
    try:
        response = requests.get(
            f"{AIRFLOW_API_URL}/dags/{dag_id}",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Airflow API error: {str(e)}")

@app.post("/dags/{dag_id}/trigger")
async def trigger_dag(dag_id: str, conf: Optional[Dict[str, Any]] = None):
    """Trigger a DAG run"""
    try:
        payload = {"conf": conf or {}}
        response = requests.post(
            f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            json=payload,
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Airflow API error: {str(e)}")


# REVIEW: Dynamic DAG generation currently writes files directly into the
# Airflow DAGs folder without linting or syntax validation. A failed write will
# silently produce an unusable DAG until the scheduler is restarted. Consider
# validating rendered code with `python -m py_compile` and refreshing Airflow.

@app.get("/dags/{dag_id}/runs")
async def get_dag_runs(dag_id: str, limit: int = 10):
    """Get recent runs of a DAG"""
    try:
        response = requests.get(
            f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns",
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            params={"limit": limit, "order_by": "-execution_date"},
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Airflow API error: {str(e)}")

@app.post("/dags/create")
async def create_dag(config: DAGConfig):
    """Create a new DAG from configuration"""
    try:
        dag_template = generate_dag_code(config)
        
        # Write DAG file
        dag_path = f"/app/dags/{config.dag_id}.py"
        with open(dag_path, 'w') as f:
            f.write(dag_template)
        
        return {"message": f"DAG {config.dag_id} created successfully", "path": dag_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def generate_dag_code(config: DAGConfig) -> str:
    """Generate Python DAG code from configuration"""
    
    source_type = config.source_type
    source_config = config.source_config
    
    if source_type == 'api':
        extract_code = f'''
    # Extract from API
    import requests
    response = requests.get("{source_config.get('url', '')}")
    data = response.json()
    if "{source_config.get('data_key', '')}":
        data = data["{source_config.get('data_key', '')}"]
    df = pd.DataFrame(data)
'''
    elif source_type == 'file':
        extract_code = f'''
    # Extract from file
    file_path = "{source_config.get('file_path', '')}"
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith('.json'):
        df = pd.read_json(file_path)
    else:
        df = pd.read_parquet(file_path)
'''
    else:
        extract_code = '''
    # Custom extraction logic
    df = pd.DataFrame()
'''

    # REVIEW: User-provided values are interpolated directly into the template,
    # which allows Python injection if untrusted input is passed. Sanitizing
    # strings or using a safer templating library would reduce that risk.
    
    dag_code = f'''"""
Auto-generated DAG: {config.dag_id}
Description: {config.description}
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import json

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

def extract_and_load():
    """Extract data and load to DuckDB"""
    {extract_code}
    
    # Load to DuckDB
    conn = duckdb.connect('/opt/airflow/data/warehouse.duckdb')
    conn.execute("CREATE TABLE IF NOT EXISTS {config.target_table} AS SELECT * FROM df WHERE 1=0")
    conn.execute("INSERT INTO {config.target_table} SELECT * FROM df")
    conn.close()
    
    return f"Loaded {{len(df)}} rows to {config.target_table}"

with DAG(
    '{config.dag_id}',
    default_args=default_args,
    description='{config.description}',
    schedule_interval='{config.schedule}',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['auto-generated'],
) as dag:
    
    extract_load_task = PythonOperator(
        task_id='extract_and_load',
        python_callable=extract_and_load,
    )
'''
    return dag_code

# ============== DBT Integration Endpoints ==============

@app.get("/dbt/models")
async def list_dbt_models():
    """List DBT models"""
    try:
        models_path = Path("/app/dbt/models")
        if not models_path.exists():
            return {"models": []}
        
        models = []
        for sql_file in models_path.glob("**/*.sql"):
            models.append({
                "name": sql_file.stem,
                "path": str(sql_file.relative_to(models_path))
            })
        
        return {"models": models}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dbt/models/{model_name}")
async def get_dbt_model(model_name: str):
    """Get DBT model content"""
    try:
        models_path = Path("/app/dbt/models")
        
        # Search for the model
        for sql_file in models_path.glob(f"**/{model_name}.sql"):
            with open(sql_file, 'r') as f:
                content = f.read()
            return {"name": model_name, "content": content}
        
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# REVIEW: The Yahoo Finance endpoints assume outbound internet and may stall on
# rate limits. Consider adding shorter timeouts and circuit breakers so the API
# process does not block under poor network conditions.

# ============== Yahoo Finance Endpoints ==============

class YahooFinanceConfig(BaseModel):
    symbols: List[str]
    period: str = "1mo"  # 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max

class YahooFinancePipelineConfig(BaseModel):
    symbols: List[str]
    schedule: str  # Cron expression
    period: str = "1mo"

class CronParseRequest(BaseModel):
    natural_language: str

# Ollama configuration
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")

def detect_ollama_host():
    """Detect Ollama host - handles Docker vs local development"""
    import platform

    # Try docker service name first (for containerized deployment)
    hosts_to_try = [
        os.getenv("OLLAMA_URL", "http://ollama:11434"),
        "http://ollama:11434",
        "http://host.docker.internal:11434",  # Docker on Mac/Windows
        "http://localhost:11434",  # Local development
    ]

    for host in hosts_to_try:
        try:
            response = requests.get(f"{host}/api/tags", timeout=2)
            if response.status_code == 200:
                print(f"[Ollama] Connected to {host}")
                return host
        except:
            continue

    return None

@app.post("/yahoo-finance/fetch")
async def fetch_yahoo_finance_now(config: YahooFinanceConfig):
    """
    Fetch stock data from Yahoo Finance immediately (test/preview mode).
    Returns the data without saving to database.
    """
    try:
        # Import yfinance here to handle cases where it's not installed
        try:
            import yfinance as yf
        except ImportError:
            raise HTTPException(
                status_code=500,
                detail="yfinance not installed. Run: pip install yfinance"
            )

        # Use yf.download() which is more reliable than Ticker().history()
        # Note: yfinance >= 0.2.40 doesn't work with custom requests.Session
        all_data = []
        errors = []

        def process_yf_dataframe(df, symbol):
            """Process yfinance DataFrame, handling multi-index columns"""
            if df.empty:
                return None

            # Flatten multi-index columns if present
            if isinstance(df.columns, pd.MultiIndex):
                # Get the level that contains the metric names (Open, High, Low, Close, Volume)
                df.columns = df.columns.get_level_values(-1)

            df = df.reset_index()
            df['symbol'] = symbol

            # Standardize column names
            df.columns = [str(c).lower().replace(' ', '_') for c in df.columns]

            # Remove any duplicate columns by keeping first occurrence
            df = df.loc[:, ~df.columns.duplicated()]

            return df

        # Try bulk download first (faster and more reliable)
        try:
            if len(config.symbols) == 1:
                # Single symbol
                symbol = config.symbols[0]
                df = yf.download(
                    tickers=symbol,
                    period=config.period,
                    auto_adjust=True,
                    progress=False
                )
                processed = process_yf_dataframe(df, symbol)
                if processed is not None and not processed.empty:
                    all_data.append(processed)
                else:
                    errors.append(f"No data for {symbol}")
            else:
                # Multiple symbols - use group_by='ticker'
                df = yf.download(
                    tickers=config.symbols,
                    period=config.period,
                    group_by='ticker',
                    auto_adjust=True,
                    progress=False,
                    threads=True
                )

                if not df.empty:
                    for symbol in config.symbols:
                        try:
                            if symbol in df.columns.get_level_values(0):
                                symbol_df = df[symbol].copy()
                                processed = process_yf_dataframe(symbol_df, symbol)
                                if processed is not None:
                                    processed = processed.dropna(subset=['open', 'high', 'low', 'close'], how='all')
                                    if not processed.empty:
                                        all_data.append(processed)
                                    else:
                                        errors.append(f"No data for {symbol}")
                        except Exception as e:
                            errors.append(f"Error processing {symbol}: {str(e)}")
        except Exception as e:
            errors.append(f"Bulk download failed: {str(e)}")

        if not all_data:
            raise HTTPException(
                status_code=400,
                detail=f"No data fetched. Errors: {errors}"
            )

        df = pd.concat(all_data, ignore_index=True)

        return {
            "data": json.loads(df.to_json(orient='records', date_format='iso')),
            "columns": list(df.columns),
            "row_count": len(df),
            "symbols_fetched": list(df['symbol'].unique()),
            "errors": errors if errors else None
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/yahoo-finance/save")
async def save_yahoo_finance_data(config: YahooFinanceConfig):
    """
    Fetch Yahoo Finance data and save to DuckDB with Iceberg-style versioning.
    """
    try:
        import yfinance as yf

        # Use yf.download() - more reliable than Ticker().history()
        # Note: yfinance >= 0.2.40 doesn't work with custom requests.Session
        all_data = []

        def process_yf_dataframe(df, symbol):
            """Process yfinance DataFrame, handling multi-index columns"""
            if df.empty:
                return None

            print(f"[YF Process] Original columns type: {type(df.columns)}")
            print(f"[YF Process] Original columns: {list(df.columns)}")

            # Flatten multi-index columns if present
            if isinstance(df.columns, pd.MultiIndex):
                # Get the level that contains the metric names (Open, High, Low, Close, Volume)
                # Usually it's the last level
                df.columns = df.columns.get_level_values(-1)
                print(f"[YF Process] Flattened columns: {list(df.columns)}")

            df = df.reset_index()
            df['symbol'] = symbol
            df['fetch_timestamp'] = datetime.now().isoformat()

            print(f"[YF Process] After reset_index columns: {list(df.columns)}")

            # Standardize column names
            df.columns = [str(c).lower().replace(' ', '_') for c in df.columns]

            # Remove any duplicate columns
            df = df.loc[:, ~df.columns.duplicated()]

            print(f"[YF Process] Final columns: {list(df.columns)}")
            print(f"[YF Process] DataFrame shape: {df.shape}")

            return df

        try:
            if len(config.symbols) == 1:
                symbol = config.symbols[0]
                df = yf.download(
                    tickers=symbol,
                    period=config.period,
                    auto_adjust=True,
                    progress=False
                )
                processed = process_yf_dataframe(df, symbol)
                if processed is not None and not processed.empty:
                    all_data.append(processed)
            else:
                df = yf.download(
                    tickers=config.symbols,
                    period=config.period,
                    group_by='ticker',
                    auto_adjust=True,
                    progress=False,
                    threads=True
                )

                if not df.empty:
                    for symbol in config.symbols:
                        try:
                            if symbol in df.columns.get_level_values(0):
                                symbol_df = df[symbol].copy()
                                processed = process_yf_dataframe(symbol_df, symbol)
                                if processed is not None:
                                    processed = processed.dropna(subset=['open', 'high', 'low', 'close'], how='all')
                                    if not processed.empty:
                                        all_data.append(processed)
                        except:
                            continue
        except:
            pass

        if not all_data:
            raise HTTPException(status_code=400, detail="No data fetched from Yahoo Finance")

        df = pd.concat(all_data, ignore_index=True)

        # Log columns for debugging
        print(f"[Yahoo Finance Save] DataFrame columns: {list(df.columns)}")
        print(f"[Yahoo Finance Save] DataFrame shape: {df.shape}")

        # Save to Delta Lake with ACID transactions
        from deltalake import write_deltalake, DeltaTable

        delta_path = "/app/data/delta/stock_prices"
        os.makedirs(os.path.dirname(delta_path), exist_ok=True)

        # Add metadata columns
        df['ingestion_date'] = datetime.now().date()
        df['ingestion_timestamp'] = datetime.now().isoformat()

        # Ensure date column is datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        # Write to Delta Lake
        try:
            delta_log_path = os.path.join(delta_path, '_delta_log')
            if os.path.exists(delta_path) and os.path.exists(delta_log_path):
                print("[Delta Lake] Appending to existing table...")
                write_deltalake(
                    delta_path,
                    df,
                    mode="append",
                    schema_mode="merge"
                )
            else:
                print("[Delta Lake] Creating new Delta table...")
                write_deltalake(
                    delta_path,
                    df,
                    mode="overwrite",
                    partition_by=["symbol"]
                )

            # Get Delta table info
            dt = DeltaTable(delta_path)
            version = dt.version()

            print(f"[Delta Lake] Saved successfully. Version: {version}")

        except Exception as e:
            print(f"[Delta Lake] Error: {e}")
            raise HTTPException(status_code=500, detail=f"Delta Lake error: {str(e)}")

        # Sync to DuckDB for querying
        conn = get_db_connection(read_only=False)

        try:
            # Try using delta extension
            conn.execute("INSTALL delta;")
            conn.execute("LOAD delta;")
            conn.execute(f"""
                CREATE OR REPLACE TABLE stock_prices AS
                SELECT * FROM delta_scan('{delta_path}')
            """)
        except Exception as e:
            print(f"[DuckDB] Delta extension failed, using parquet fallback: {e}")
            # Fallback to reading parquet files directly
            conn.execute(f"""
                CREATE OR REPLACE TABLE stock_prices AS
                SELECT * FROM read_parquet('{delta_path}/*.parquet')
            """)

        count = conn.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
        conn.close()

        return {
            "message": "Data saved successfully with Delta Lake ACID transaction",
            "delta_path": delta_path,
            "delta_version": version,
            "records_saved": len(df),
            "total_records": count
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/yahoo-finance/create-pipeline")
async def create_yahoo_finance_pipeline(config: YahooFinancePipelineConfig):
    """
    Create a scheduled Airflow DAG for Yahoo Finance data fetching.
    """
    try:
        dag_id = "yahoo_finance_custom"
        symbols_str = ",".join(config.symbols)

        dag_code = f'''"""
Auto-generated Yahoo Finance DAG
Created: {datetime.now().isoformat()}
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import requests
import os
import yfinance as yf

# Configuration
SYMBOLS = {config.symbols}
PERIOD = "{config.period}"
DUCKDB_PATH = '/opt/airflow/data/warehouse.duckdb'
ICEBERG_DIR = '/opt/airflow/data/iceberg/stock_prices/data'

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}}

def fetch_and_save_stock_data(**context):
    """Fetch stock data and save with Iceberg-style versioning."""
    all_data = []

    def process_df(df, symbol):
        if df.empty:
            return None
        # Flatten multi-index columns if present
        if hasattr(df.columns, 'nlevels') and df.columns.nlevels > 1:
            df.columns = df.columns.get_level_values(-1)
        df = df.reset_index()
        df['symbol'] = symbol
        df['fetch_timestamp'] = datetime.now().isoformat()
        df.columns = [str(c).lower().replace(' ', '_') for c in df.columns]
        df = df.loc[:, ~df.columns.duplicated()]
        return df

    try:
        if len(SYMBOLS) == 1:
            symbol = SYMBOLS[0]
            df = yf.download(tickers=symbol, period=PERIOD, auto_adjust=True, progress=False)
            processed = process_df(df, symbol)
            if processed is not None and not processed.empty:
                all_data.append(processed)
        else:
            df = yf.download(tickers=SYMBOLS, period=PERIOD, group_by='ticker', auto_adjust=True, progress=False, threads=True)
            if not df.empty:
                for symbol in SYMBOLS:
                    try:
                        if symbol in df.columns.get_level_values(0):
                            symbol_df = df[symbol].copy()
                            processed = process_df(symbol_df, symbol)
                            if processed is not None:
                                processed = processed.dropna(subset=['open', 'high', 'low', 'close'], how='all')
                                if not processed.empty:
                                    all_data.append(processed)
                    except Exception as e:
                        print(f"Error processing {{symbol}}: {{e}}")
    except Exception as e:
        print(f"Download failed: {{e}}")

    if not all_data:
        raise ValueError("No data fetched from Yahoo Finance")

    df = pd.concat(all_data, ignore_index=True)

    # Save to Iceberg-style Parquet
    os.makedirs(ICEBERG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    snapshot_id = int(datetime.now().timestamp() * 1000)
    parquet_path = f"{{ICEBERG_DIR}}/snapshot_{{snapshot_id}}_{{timestamp}}.parquet"

    df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
    print(f"Saved: {{parquet_path}}")

    # Load to DuckDB with dynamic column handling
    conn = duckdb.connect(DUCKDB_PATH, read_only=False)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            date TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, symbol VARCHAR, fetch_timestamp VARCHAR,
            ingestion_date DATE DEFAULT CURRENT_DATE
        )
    """)

    # Get available columns from parquet
    parquet_cols = conn.execute(f"SELECT * FROM read_parquet('{{parquet_path}}') LIMIT 0").description
    available = [col[0] for col in parquet_cols]

    # Get table columns
    table_cols = conn.execute("SELECT * FROM stock_prices LIMIT 0").description
    table_col_names = [col[0] for col in table_cols]

    # Build fully dynamic INSERT - only use columns that exist in BOTH
    base_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']
    insert_cols = []
    select_parts = []

    for col in base_cols:
        if col in table_col_names:
            insert_cols.append(col)
            if col in available:
                select_parts.append(f'"{col}"')
            else:
                select_parts.append('NULL')

    # Add optional columns only if they exist in both
    if 'fetch_timestamp' in table_col_names and 'fetch_timestamp' in available:
        insert_cols.append('fetch_timestamp')
        select_parts.append('"fetch_timestamp"')

    if 'ingestion_date' in table_col_names:
        insert_cols.append('ingestion_date')
        select_parts.append('CURRENT_DATE')

    insert_clause = ', '.join(insert_cols)
    select_clause = ', '.join(select_parts)

    conn.execute(f"""
        INSERT INTO stock_prices ({{insert_clause}})
        SELECT {{select_clause}}
        FROM read_parquet('{{parquet_path}}')
    """)

    count = conn.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
    conn.close()

    print(f"Total records in stock_prices: {{count}}")
    return count

with DAG(
    '{dag_id}',
    default_args=default_args,
    description='Yahoo Finance data pipeline - Symbols: {symbols_str}',
    schedule_interval='{config.schedule}',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['yahoo-finance', 'auto-generated'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_save_stock_data',
        python_callable=fetch_and_save_stock_data,
    )
'''

        # Write DAG file
        dag_path = f"/app/dags/{dag_id}.py"
        with open(dag_path, 'w') as f:
            f.write(dag_code)

        return {
            "message": f"Pipeline {dag_id} created successfully",
            "dag_id": dag_id,
            "symbols": config.symbols,
            "schedule": config.schedule,
            "dag_path": dag_path
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============== Natural Language Cron Parser ==============

CRON_PATTERNS = {
    # Simple patterns - regex matching
    r"every\s+minute": "* * * * *",
    r"every\s+hour": "0 * * * *",
    r"every\s+day\s+at\s+midnight": "0 0 * * *",
    r"daily\s+at\s+midnight": "0 0 * * *",
    r"hourly": "0 * * * *",
    r"daily": "0 0 * * *",
    r"weekly": "0 0 * * 0",
    r"monthly": "0 0 1 * *",
    r"yearly": "0 0 1 1 *",
    r"annually": "0 0 1 1 *",

    # Time-specific patterns
    r"every\s+day\s+at\s+(\d{1,2})\s*(am|pm)?": None,  # Handle dynamically
    r"daily\s+at\s+(\d{1,2})\s*(am|pm)?": None,
    r"every\s+(\d+)\s+minutes?": None,
    r"every\s+(\d+)\s+hours?": None,

    # Weekday patterns
    r"every\s+weekday": "0 9 * * 1-5",
    r"weekdays\s+at\s+(\d{1,2})\s*(am|pm)?": None,
    r"monday\s+to\s+friday": "0 9 * * 1-5",
}

import re

def parse_time(hour_str: str, ampm: str = None) -> int:
    """Convert hour string with optional AM/PM to 24-hour format."""
    hour = int(hour_str)
    if ampm:
        ampm = ampm.lower()
        if ampm == 'pm' and hour != 12:
            hour += 12
        elif ampm == 'am' and hour == 12:
            hour = 0
    return hour

def parse_cron_regex(text: str) -> Optional[str]:
    """Parse cron expression using regex patterns."""
    text = text.lower().strip()

    # Direct simple patterns
    simple_patterns = {
        "every minute": "* * * * *",
        "every hour": "0 * * * *",
        "hourly": "0 * * * *",
        "daily": "0 0 * * *",
        "weekly": "0 0 * * 0",
        "monthly": "0 0 1 * *",
        "yearly": "0 0 1 1 *",
        "every weekday": "0 9 * * 1-5",
        "weekdays": "0 9 * * 1-5",
        "every day at midnight": "0 0 * * *",
        "daily at midnight": "0 0 * * *",
    }

    if text in simple_patterns:
        return simple_patterns[text]

    # Pattern: "every day at X (am/pm)" or "daily at X (am/pm)"
    match = re.search(r"(?:every\s+day|daily)\s+at\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", text)
    if match:
        hour = parse_time(match.group(1), match.group(3))
        minute = int(match.group(2)) if match.group(2) else 0
        return f"{minute} {hour} * * *"

    # Pattern: "at X (am/pm)"
    match = re.search(r"^at\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$", text)
    if match:
        hour = parse_time(match.group(1), match.group(3))
        minute = int(match.group(2)) if match.group(2) else 0
        return f"{minute} {hour} * * *"

    # Pattern: "every N minutes"
    match = re.search(r"every\s+(\d+)\s+minutes?", text)
    if match:
        minutes = int(match.group(1))
        return f"*/{minutes} * * * *"

    # Pattern: "every N hours"
    match = re.search(r"every\s+(\d+)\s+hours?", text)
    if match:
        hours = int(match.group(1))
        return f"0 */{hours} * * *"

    # Pattern: "weekdays at X (am/pm)"
    match = re.search(r"weekdays?\s+at\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", text)
    if match:
        hour = parse_time(match.group(1), match.group(3))
        minute = int(match.group(2)) if match.group(2) else 0
        return f"{minute} {hour} * * 1-5"

    # Pattern: "every day except sunday at X" or "everyday except sunday at X"
    match = re.search(r"every\s*day\s+except\s+sunday\s+(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", text)
    if match:
        hour = parse_time(match.group(1), match.group(3))
        minute = int(match.group(2)) if match.group(2) else 0
        return f"{minute} {hour} * * 1-6"  # Mon-Sat

    # Pattern: "every day except saturday at X"
    match = re.search(r"every\s*day\s+except\s+saturday\s+(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", text)
    if match:
        hour = parse_time(match.group(1), match.group(3))
        minute = int(match.group(2)) if match.group(2) else 0
        return f"{minute} {hour} * * 0-5"  # Sun-Fri

    # Pattern: "every day except weekends at X"
    match = re.search(r"every\s*day\s+except\s+weekends?\s+(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", text)
    if match:
        hour = parse_time(match.group(1), match.group(3))
        minute = int(match.group(2)) if match.group(2) else 0
        return f"{minute} {hour} * * 1-5"  # Mon-Fri

    # Pattern: specific day at time
    days = {
        'sunday': '0', 'monday': '1', 'tuesday': '2', 'wednesday': '3',
        'thursday': '4', 'friday': '5', 'saturday': '6'
    }

    for day, num in days.items():
        match = re.search(rf"(?:every\s+)?{day}s?\s+at\s+(\d{{1,2}})(?::(\d{{2}}))?\s*(am|pm)?", text)
        if match:
            hour = parse_time(match.group(1), match.group(3))
            minute = int(match.group(2)) if match.group(2) else 0
            return f"{minute} {hour} * * {num}"

    # Pattern: Time range with interval "X-Ypm every N minutes"
    match = re.search(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*-\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s+every\s+(\d+)\s+minutes?", text)
    if match:
        start_hour = parse_time(match.group(1), match.group(3))
        end_hour = parse_time(match.group(4), match.group(6))
        interval = int(match.group(7))
        return f"*/{interval} {start_hour}-{end_hour} * * *"

    return None

async def parse_cron_with_ollama(text: str) -> tuple[Optional[str], str]:
    """Use Ollama/Llama to parse complex natural language to cron. Returns (cron, status_message)."""
    host = detect_ollama_host()

    if not host:
        return None, "Ollama service not connected - install llama3.2 model for complex patterns"

    # Check if model is available
    try:
        tags_response = requests.get(f"{host}/api/tags", timeout=5)
        if tags_response.status_code == 200:
            models = [m.get("name", "") for m in tags_response.json().get("models", [])]
            if not any("llama" in m.lower() for m in models):
                return None, f"No Llama model installed. Run: docker exec ollama ollama pull llama3.2"
    except:
        pass

    prompt = f"""Convert this schedule to a cron expression. Only output the 5-field cron expression, nothing else.

Schedule: "{text}"

Rules:
- Cron format: minute hour day-of-month month day-of-week
- Days: 0=Sunday, 1=Monday, 2=Tuesday, 3=Wednesday, 4=Thursday, 5=Friday, 6=Saturday
- "every day except sunday" means days 1-6 (Mon-Sat)
- "1-3pm every 30 minutes" means minute=*/30, hour=13-15

Examples:
"every day at 5pm" = 0 17 * * *
"monday at 9am" = 0 9 * * 1
"weekdays at 6pm" = 0 18 * * 1-5
"every day except sunday at 10pm" = 0 22 * * 1-6
"1pm-3pm every 30 minutes" = */30 13-15 * * *

Output only the cron expression:"""

    try:
        response = requests.post(
            f"{host}/api/generate",
            json={
                "model": "llama3.2",
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 50}
            },
            timeout=30
        )

        if response.status_code == 200:
            result = response.json().get("response", "").strip()

            # Clean up - extract just the cron expression
            lines = result.split('\n')
            for line in lines:
                line = line.strip()
                # Remove common prefixes
                line = re.sub(r'^(cron:|output:|result:|answer:)\s*', '', line, flags=re.IGNORECASE)
                parts = line.split()
                if len(parts) == 5:
                    # Validate each part looks like cron
                    valid = True
                    for part in parts:
                        if not re.match(r'^[\d\*\/\-\,]+$', part):
                            valid = False
                            break
                    if valid:
                        print(f"[Ollama] Parsed '{text}' -> '{line}'")
                        return line, "ollama"

            return None, f"Ollama returned invalid cron: {result[:100]}"
        else:
            return None, f"Ollama API error: {response.status_code}"

    except requests.exceptions.Timeout:
        return None, "Ollama request timed out - model may be loading"
    except Exception as e:
        return None, f"Ollama error: {str(e)}"

@app.post("/cron/parse")
async def parse_cron_expression(request: CronParseRequest):
    """
    Parse natural language schedule to cron expression.
    Tier 1: Regex patterns for common cases
    Tier 2: Ollama/Llama for complex patterns
    """
    text = request.natural_language.strip()

    # Tier 1: Try regex patterns first
    cron = parse_cron_regex(text)
    if cron:
        return {
            "cron": cron,
            "source": "regex",
            "input": text
        }

    # Tier 2: Try Ollama for complex patterns
    cron, status = await parse_cron_with_ollama(text)
    if cron:
        return {
            "cron": cron,
            "source": "ollama",
            "input": text
        }

    # If Ollama didn't work, provide helpful error
    raise HTTPException(
        status_code=400,
        detail=f"Could not parse: '{text}'. {status}. Try formats like 'daily at 5pm', 'every monday at 9am', 'every day except sunday at 10pm'"
    )

@app.get("/cron/examples")
async def get_cron_examples():
    """Get example natural language to cron conversions."""
    return {
        "examples": [
            {"natural": "every hour", "cron": "0 * * * *"},
            {"natural": "daily at 5pm", "cron": "0 17 * * *"},
            {"natural": "every day at 9:30am", "cron": "30 9 * * *"},
            {"natural": "weekdays at 6pm", "cron": "0 18 * * 1-5"},
            {"natural": "every monday at 10am", "cron": "0 10 * * 1"},
            {"natural": "every 15 minutes", "cron": "*/15 * * * *"},
            {"natural": "every 2 hours", "cron": "0 */2 * * *"},
            {"natural": "every day except sunday at 10pm", "cron": "0 22 * * 1-6"},
            {"natural": "monthly", "cron": "0 0 1 * *"},
        ]
    }

# ============== Delta Lake Endpoints ==============

@app.get("/delta/tables")
async def list_delta_tables():
    """List all Delta Lake tables."""
    try:
        delta_base = "/app/data/delta"
        tables = []

        if os.path.exists(delta_base):
            for item in os.listdir(delta_base):
                table_path = os.path.join(delta_base, item)
                delta_log = os.path.join(table_path, '_delta_log')

                if os.path.isdir(table_path) and os.path.exists(delta_log):
                    try:
                        from deltalake import DeltaTable
                        dt = DeltaTable(table_path)
                        tables.append({
                            "name": item,
                            "path": table_path,
                            "version": dt.version(),
                            "files": len(dt.files())
                        })
                    except:
                        tables.append({
                            "name": item,
                            "path": table_path,
                            "version": "unknown",
                            "files": 0
                        })

        return {"tables": tables}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/delta/{table_name}/history")
async def get_delta_history(table_name: str, limit: int = 20):
    """
    Get version history for a Delta Lake table.
    Shows all commits with timestamps and operations.
    """
    try:
        from deltalake import DeltaTable

        delta_path = f"/app/data/delta/{table_name}"

        if not os.path.exists(delta_path):
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

        dt = DeltaTable(delta_path)
        history = dt.history(limit=limit)

        return {
            "table": table_name,
            "current_version": dt.version(),
            "history": history
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/delta/{table_name}/query")
async def query_delta_table(
    table_name: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
    limit: int = 100
):
    """
    Query a Delta Lake table with optional time travel.

    Time travel options:
    - version: Query specific version number (e.g., version=5)
    - timestamp: Query as of timestamp (e.g., timestamp=2024-01-15T10:00:00)

    Examples:
    - /delta/stock_prices/query - Current data
    - /delta/stock_prices/query?version=3 - Data at version 3
    - /delta/stock_prices/query?timestamp=2024-01-15 - Data as of date
    """
    try:
        from deltalake import DeltaTable

        delta_path = f"/app/data/delta/{table_name}"

        if not os.path.exists(delta_path):
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

        # Load table with time travel if specified
        if version is not None:
            dt = DeltaTable(delta_path, version=version)
            time_travel_info = f"version {version}"
        elif timestamp:
            dt = DeltaTable(delta_path, timestamp=timestamp)
            time_travel_info = f"timestamp {timestamp}"
        else:
            dt = DeltaTable(delta_path)
            time_travel_info = "current"

        # Convert to pandas for query
        df = dt.to_pandas()

        # Apply limit
        if limit and limit < len(df):
            df = df.head(limit)

        return {
            "table": table_name,
            "version": dt.version(),
            "time_travel": time_travel_info,
            "columns": list(df.columns),
            "row_count": len(df),
            "data": df.to_dict('records')
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/delta/{table_name}/restore")
async def restore_delta_version(table_name: str, target_version: int):
    """
    Restore a Delta Lake table to a previous version.
    This creates a new version that matches the target version's data.
    """
    try:
        from deltalake import DeltaTable

        delta_path = f"/app/data/delta/{table_name}"

        if not os.path.exists(delta_path):
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

        dt = DeltaTable(delta_path)
        current_version = dt.version()

        if target_version > current_version:
            raise HTTPException(
                status_code=400,
                detail=f"Target version {target_version} is greater than current version {current_version}"
            )

        # Restore to target version
        dt.restore(target_version)

        new_version = dt.version()

        return {
            "message": f"Successfully restored {table_name} to version {target_version}",
            "previous_version": current_version,
            "restored_to": target_version,
            "new_version": new_version
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/delta/{table_name}/optimize")
async def optimize_delta_table(table_name: str):
    """
    Optimize a Delta Lake table by compacting small files.
    Improves query performance.
    """
    try:
        from deltalake import DeltaTable

        delta_path = f"/app/data/delta/{table_name}"

        if not os.path.exists(delta_path):
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

        dt = DeltaTable(delta_path)
        files_before = len(dt.files())

        # Run compaction
        result = dt.optimize.compact()

        files_after = len(dt.files())

        return {
            "message": f"Optimization complete for {table_name}",
            "files_before": files_before,
            "files_after": files_after,
            "version": dt.version(),
            "metrics": result
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/delta/{table_name}/vacuum")
async def vacuum_delta_table(table_name: str, retention_hours: int = 168):
    """
    Remove old files no longer referenced by the table.
    Default retention is 7 days (168 hours).

    WARNING: This permanently deletes old data files.
    """
    try:
        from deltalake import DeltaTable

        delta_path = f"/app/data/delta/{table_name}"

        if not os.path.exists(delta_path):
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

        dt = DeltaTable(delta_path)

        # Vacuum with retention
        deleted_files = dt.vacuum(
            retention_hours=retention_hours,
            enforce_retention_duration=False,
            dry_run=False
        )

        return {
            "message": f"Vacuum complete for {table_name}",
            "retention_hours": retention_hours,
            "deleted_files": deleted_files
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Legacy Iceberg endpoints (redirect to Delta)
@app.get("/iceberg/snapshots")
async def list_iceberg_snapshots_legacy(table_name: str = "stock_prices"):
    """Legacy endpoint - redirects to Delta Lake history."""
    return await get_delta_history(table_name)


@app.get("/iceberg/query")
async def query_iceberg_snapshot_legacy(
    table_name: str = "stock_prices",
    snapshot_id: Optional[str] = None,
    limit: int = 100
):
    """Legacy endpoint - redirects to Delta Lake query."""
    version = int(snapshot_id) if snapshot_id and snapshot_id.isdigit() else None
    return await query_delta_table(table_name, version=version, limit=limit)

# ============== DBT Templates ==============

DBT_TEMPLATES = {
    "daily_returns": {
        "name": "Daily Returns",
        "description": "Calculate daily percentage returns for a price column",
        "required_columns": ["date_column", "price_column", "group_column"],
        "template": """
-- Daily Returns Model
-- Generated: {{{{ current_timestamp() }}}}

SELECT
    {{{{ group_column }}}},
    {{{{ date_column }}}},
    {{{{ price_column }}}},
    LAG({{{{ price_column }}}}) OVER (PARTITION BY {{{{ group_column }}}} ORDER BY {{{{ date_column }}}}) as prev_price,
    ({{{{ price_column }}}} - LAG({{{{ price_column }}}}) OVER (PARTITION BY {{{{ group_column }}}} ORDER BY {{{{ date_column }}}})) /
        NULLIF(LAG({{{{ price_column }}}}) OVER (PARTITION BY {{{{ group_column }}}} ORDER BY {{{{ date_column }}}}), 0) * 100 as daily_return_pct
FROM {{{{ ref('{source_table}') }}}}
ORDER BY {{{{ group_column }}}}, {{{{ date_column }}}}
"""
    },
    "moving_average": {
        "name": "Moving Average",
        "description": "Calculate N-day moving average for a numeric column",
        "required_columns": ["date_column", "value_column", "group_column", "window_size"],
        "template": """
-- Moving Average Model
-- Generated: {{{{ current_timestamp() }}}}

SELECT
    {{{{ group_column }}}},
    {{{{ date_column }}}},
    {{{{ value_column }}}},
    AVG({{{{ value_column }}}}) OVER (
        PARTITION BY {{{{ group_column }}}}
        ORDER BY {{{{ date_column }}}}
        ROWS BETWEEN {{{{ window_size }}}} PRECEDING AND CURRENT ROW
    ) as moving_avg_{{{{ window_size }}}}
FROM {{{{ ref('{source_table}') }}}}
ORDER BY {{{{ group_column }}}}, {{{{ date_column }}}}
"""
    },
    "volatility": {
        "name": "Volatility (Standard Deviation)",
        "description": "Calculate rolling volatility for a numeric column",
        "required_columns": ["date_column", "value_column", "group_column", "window_size"],
        "template": """
-- Volatility Model
-- Generated: {{{{ current_timestamp() }}}}

SELECT
    {{{{ group_column }}}},
    {{{{ date_column }}}},
    {{{{ value_column }}}},
    STDDEV({{{{ value_column }}}}) OVER (
        PARTITION BY {{{{ group_column }}}}
        ORDER BY {{{{ date_column }}}}
        ROWS BETWEEN {{{{ window_size }}}} PRECEDING AND CURRENT ROW
    ) as volatility_{{{{ window_size }}}}
FROM {{{{ ref('{source_table}') }}}}
ORDER BY {{{{ group_column }}}}, {{{{ date_column }}}}
"""
    },
    "yoy_growth": {
        "name": "Year-over-Year Growth",
        "description": "Calculate year-over-year growth rate",
        "required_columns": ["date_column", "value_column", "group_column"],
        "template": """
-- Year-over-Year Growth Model
-- Generated: {{{{ current_timestamp() }}}}

WITH lagged AS (
    SELECT
        {{{{ group_column }}}},
        {{{{ date_column }}}},
        {{{{ value_column }}}},
        LAG({{{{ value_column }}}}, 365) OVER (
            PARTITION BY {{{{ group_column }}}}
            ORDER BY {{{{ date_column }}}}
        ) as value_1y_ago
    FROM {{{{ ref('{source_table}') }}}}
)

SELECT
    {{{{ group_column }}}},
    {{{{ date_column }}}},
    {{{{ value_column }}}},
    value_1y_ago,
    ({{{{ value_column }}}} - value_1y_ago) / NULLIF(value_1y_ago, 0) * 100 as yoy_growth_pct
FROM lagged
WHERE value_1y_ago IS NOT NULL
ORDER BY {{{{ group_column }}}}, {{{{ date_column }}}}
"""
    },
    "aggregation": {
        "name": "Group Aggregation",
        "description": "Aggregate data by group with common statistics",
        "required_columns": ["group_column", "value_column"],
        "template": """
-- Aggregation Model
-- Generated: {{{{ current_timestamp() }}}}

SELECT
    {{{{ group_column }}}},
    COUNT(*) as record_count,
    MIN({{{{ value_column }}}}) as min_value,
    MAX({{{{ value_column }}}}) as max_value,
    AVG({{{{ value_column }}}}) as avg_value,
    STDDEV({{{{ value_column }}}}) as stddev_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{{{ value_column }}}}) as median_value
FROM {{{{ ref('{source_table}') }}}}
GROUP BY {{{{ group_column }}}}
ORDER BY {{{{ group_column }}}}
"""
    }
}

class DBTTransformRequest(BaseModel):
    template_id: str
    source_table: str
    output_model_name: str
    column_mappings: Dict[str, str]

@app.get("/dbt/templates")
async def list_dbt_templates():
    """List available DBT transformation templates."""
    templates = []
    for tid, template in DBT_TEMPLATES.items():
        templates.append({
            "id": tid,
            "name": template["name"],
            "description": template["description"],
            "required_columns": template["required_columns"]
        })
    return {"templates": templates}

@app.get("/dbt/templates/{template_id}")
async def get_dbt_template(template_id: str):
    """Get details of a specific DBT template."""
    if template_id not in DBT_TEMPLATES:
        raise HTTPException(status_code=404, detail=f"Template {template_id} not found")

    template = DBT_TEMPLATES[template_id]
    return {
        "id": template_id,
        **template
    }

@app.post("/dbt/create-model")
async def create_dbt_model(request: DBTTransformRequest):
    """Create a new DBT model from a template."""
    try:
        if request.template_id not in DBT_TEMPLATES:
            raise HTTPException(status_code=404, detail=f"Template {request.template_id} not found")

        template = DBT_TEMPLATES[request.template_id]

        # Validate required columns are mapped
        for col in template["required_columns"]:
            if col not in request.column_mappings:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required column mapping: {col}"
                )

        # Generate the model SQL
        model_sql = template["template"].format(source_table=request.source_table)

        # Replace column placeholders
        for placeholder, actual_column in request.column_mappings.items():
            model_sql = model_sql.replace(f"{{{{ {placeholder} }}}}", actual_column)

        # Write the model file
        models_dir = Path("/app/dbt/models")
        models_dir.mkdir(parents=True, exist_ok=True)

        model_path = models_dir / f"{request.output_model_name}.sql"
        with open(model_path, 'w') as f:
            f.write(model_sql)

        return {
            "message": f"DBT model {request.output_model_name} created successfully",
            "model_path": str(model_path),
            "template_used": request.template_id,
            "sql_preview": model_sql[:500] + "..." if len(model_sql) > 500 else model_sql
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/dbt/run")
async def run_dbt():
    """Trigger DBT run (requires dbt to be installed in container)."""
    try:
        import subprocess

        result = subprocess.run(
            ["dbt", "run", "--project-dir", "/app/dbt"],
            capture_output=True,
            text=True,
            timeout=300
        )

        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr
        }

    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="DBT run timed out")
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="DBT not installed in container")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============== AI Query Endpoints (Ollama Integration) ==============

@app.post("/ai/sql")
async def natural_language_to_sql(query: SemanticQuery):
    """
    Convert natural language question to SQL using Ollama/SQLCoder.
    """
    try:
        host = detect_ollama_host()

        if not host:
            raise HTTPException(
                status_code=503,
                detail="Ollama service not available. Please ensure Ollama is running."
            )

        # Get table schemas for context
        conn = get_db_connection()
        tables = conn.execute("SHOW TABLES").fetchall()

        schema_context = ""
        for (table_name,) in tables:
            try:
                columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
                schema_context += f"\nTable: {table_name}\nColumns: "
                schema_context += ", ".join([f"{col[0]} ({col[1]})" for col in columns])
                schema_context += "\n"
            except:
                continue

        conn.close()

        prompt = f"""You are a SQL expert. Generate a DuckDB SQL query based on the user's question.
Only respond with the SQL query, nothing else. Do not include markdown formatting.

Database Schema:
{schema_context}

User Question: {query.question}

SQL Query:"""

        # Try SQLCoder first, fall back to Llama
        models_to_try = ["sqlcoder", "llama3.2", "llama2"]

        for model in models_to_try:
            try:
                response = requests.post(
                    f"{host}/api/generate",
                    json={
                        "model": model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.1}
                    },
                    timeout=60
                )

                if response.status_code == 200:
                    sql = response.json().get("response", "").strip()

                    # Clean up the response
                    sql = sql.replace("```sql", "").replace("```", "").strip()

                    return {
                        "sql": sql,
                        "model_used": model,
                        "question": query.question
                    }

            except Exception as e:
                print(f"[AI/SQL] Error with model {model}: {e}")
                continue

        raise HTTPException(
            status_code=500,
            detail="Could not generate SQL. Please ensure Ollama models are installed."
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ai/explain")
async def explain_query_results(
    sql: str = Body(...),
    question: str = Body(...),
    results: List[Dict] = Body(...)
):
    """
    Use Ollama/Llama to explain query results in natural language.
    """
    try:
        host = detect_ollama_host()

        if not host:
            return {"explanation": "AI service not available. Here are your query results."}

        # Limit results for context
        limited_results = results[:10]

        prompt = f"""You are a helpful data analyst. Explain the following query results in plain English.
Be concise and highlight key insights.

User's Question: {question}

SQL Query: {sql}

Results (first {len(limited_results)} rows):
{json.dumps(limited_results, indent=2)}

Explanation:"""

        try:
            response = requests.post(
                f"{host}/api/generate",
                json={
                    "model": "llama3.2",
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.3}
                },
                timeout=60
            )

            if response.status_code == 200:
                explanation = response.json().get("response", "").strip()
                return {"explanation": explanation}

        except:
            pass

        return {"explanation": f"Query returned {len(results)} results."}

    except Exception as e:
        return {"explanation": f"Could not generate explanation: {str(e)}"}

@app.get("/ollama/status")
async def check_ollama_status():
    """Check Ollama service status and available models."""
    host = detect_ollama_host()

    if not host:
        return {
            "status": "disconnected",
            "message": "Could not connect to Ollama service",
            "models": []
        }

    try:
        response = requests.get(f"{host}/api/tags", timeout=5)

        if response.status_code == 200:
            models = response.json().get("models", [])
            return {
                "status": "connected",
                "host": host,
                "models": [m.get("name") for m in models]
            }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "models": []
        }

@app.post("/ollama/pull")
async def pull_ollama_model(model: str = Body(..., embed=True)):
    """Pull a model to Ollama (can take several minutes)."""
    host = detect_ollama_host()

    if not host:
        raise HTTPException(status_code=503, detail="Ollama service not available")

    try:
        # This is a long-running operation
        response = requests.post(
            f"{host}/api/pull",
            json={"name": model},
            timeout=600  # 10 minute timeout
        )

        return {
            "message": f"Model {model} pull initiated",
            "status": response.status_code
        }

    except requests.exceptions.Timeout:
        return {"message": f"Model {model} pull in progress (may take several minutes)"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============== DBT Endpoints ==============

DBT_PROJECT_DIR = "/app/dbt"

@app.get("/dbt/models")
async def list_dbt_models():
    """List all DBT models organized by layer."""
    try:
        models = {
            "staging": [],
            "intermediate": [],
            "marts": []
        }

        for layer in models.keys():
            layer_dir = os.path.join(DBT_PROJECT_DIR, "models", layer)
            if os.path.exists(layer_dir):
                for f in os.listdir(layer_dir):
                    if f.endswith('.sql'):
                        model_name = f.replace('.sql', '')
                        models[layer].append({
                            "name": model_name,
                            "path": f"models/{layer}/{f}"
                        })

        return {
            "project_dir": DBT_PROJECT_DIR,
            "models": models,
            "total_models": sum(len(v) for v in models.values())
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dbt/model/{model_name}")
async def get_dbt_model(model_name: str):
    """Get the SQL content of a specific DBT model."""
    try:
        # Search in all layers
        for layer in ['staging', 'intermediate', 'marts']:
            model_path = os.path.join(DBT_PROJECT_DIR, "models", layer, f"{model_name}.sql")
            if os.path.exists(model_path):
                with open(model_path, 'r') as f:
                    content = f.read()
                return {
                    "model": model_name,
                    "layer": layer,
                    "path": model_path,
                    "sql": content
                }

        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/dbt/run")
async def run_dbt(
    selector: Optional[str] = None,
    full_refresh: bool = False
):
    """
    Run DBT models.

    Args:
        selector: DBT selector (e.g., 'staging', 'marts', 'mart_stock_summary')
        full_refresh: Force full rebuild of incremental models
    """
    import subprocess

    try:
        cmd = ["dbt", "run", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR]

        if selector:
            cmd.extend(["--select", selector])

        if full_refresh:
            cmd.append("--full-refresh")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300
        )

        return {
            "success": result.returncode == 0,
            "command": " ".join(cmd),
            "stdout": result.stdout,
            "stderr": result.stderr if result.returncode != 0 else None
        }

    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="DBT run timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/dbt/test")
async def run_dbt_tests(selector: Optional[str] = None):
    """Run DBT tests to validate data quality."""
    import subprocess

    try:
        cmd = ["dbt", "test", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR]

        if selector:
            cmd.extend(["--select", selector])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120
        )

        return {
            "success": result.returncode == 0,
            "command": " ".join(cmd),
            "stdout": result.stdout,
            "stderr": result.stderr if result.returncode != 0 else None
        }

    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="DBT test timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dbt/docs")
async def get_dbt_docs():
    """Get DBT documentation catalog if available."""
    try:
        catalog_path = os.path.join(DBT_PROJECT_DIR, "target", "catalog.json")
        manifest_path = os.path.join(DBT_PROJECT_DIR, "target", "manifest.json")

        result = {
            "catalog_exists": os.path.exists(catalog_path),
            "manifest_exists": os.path.exists(manifest_path),
        }

        if os.path.exists(manifest_path):
            import json
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
                result["models"] = list(manifest.get("nodes", {}).keys())[:20]  # First 20 nodes
                result["sources"] = list(manifest.get("sources", {}).keys())

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/dbt/lineage/{model_name}")
async def get_model_lineage(model_name: str):
    """Get the lineage (dependencies) for a DBT model."""
    try:
        manifest_path = os.path.join(DBT_PROJECT_DIR, "target", "manifest.json")

        if not os.path.exists(manifest_path):
            return {
                "message": "Run 'dbt docs generate' first to build lineage",
                "model": model_name
            }

        import json
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)

        # Find the model node
        nodes = manifest.get("nodes", {})
        model_key = None
        for key in nodes.keys():
            if model_name in key:
                model_key = key
                break

        if not model_key:
            raise HTTPException(status_code=404, detail=f"Model {model_name} not found in manifest")

        node = nodes[model_key]

        return {
            "model": model_name,
            "depends_on": node.get("depends_on", {}).get("nodes", []),
            "description": node.get("description", ""),
            "columns": list(node.get("columns", {}).keys()),
            "tags": node.get("tags", []),
            "materialized": node.get("config", {}).get("materialized", "view")
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
