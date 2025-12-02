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
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/warehouse.duckdb")
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")

# Ensure data directory exists
os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

# Models
class SQLQuery(BaseModel):
    query: str
    limit: Optional[int] = 1000  # Default limit to prevent memory issues
    offset: Optional[int] = 0     # For pagination

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
    return conn

# Ollama helper functions
def check_ollama_available():
    """Check if Ollama service is available"""
    try:
        response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=2)
        return response.status_code == 200
    except:
        return False

def ensure_ollama_model(model_name="sqlcoder"):
    """Ensure the model is pulled in Ollama"""
    try:
        # Check if model exists
        response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        if response.status_code == 200:
            models = response.json().get("models", [])
            model_names = [m.get("name", "").split(":")[0] for m in models]

            if model_name not in model_names:
                print(f"Pulling {model_name} model... This may take a few minutes on first run.")
                # Pull the model
                pull_response = requests.post(
                    f"{OLLAMA_BASE_URL}/api/pull",
                    json={"name": model_name},
                    timeout=300
                )
                return pull_response.status_code == 200
        return True
    except Exception as e:
        print(f"Error ensuring Ollama model: {e}")
        return False

def query_ollama(prompt, model="sqlcoder", system_prompt=None):
    """Query Ollama LLM"""
    try:
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0,
                "num_predict": 500
            }
        }

        if system_prompt:
            payload["system"] = system_prompt

        response = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json=payload,
            timeout=60
        )

        if response.status_code == 200:
            return response.json().get("response", "").strip()
        return None
    except Exception as e:
        print(f"Ollama query error: {e}")
        return None

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
    """Execute a SQL query against DuckDB with pagination and result limiting"""
    try:
        # Check if it's a SELECT query
        query = query_request.query.strip()
        is_select = query.upper().startswith("SELECT") or query.upper().startswith("WITH") or query.upper().startswith("SHOW") or query.upper().startswith("DESCRIBE")

        # Use read_only for SELECT, write access for modifications
        conn = get_db_connection(read_only=is_select)

        if is_select:
            # Get total count before applying limit (for pagination)
            # Wrap query in a subquery to count total rows
            count_query = f"SELECT COUNT(*) as total FROM ({query}) as subquery"
            try:
                total_rows = conn.execute(count_query).fetchone()[0]
            except:
                # If count query fails (e.g., for SHOW/DESCRIBE), fall back to basic execution
                total_rows = None

            # Apply limit and offset to the query
            # Check if query already has LIMIT clause
            query_upper = query.upper()
            has_limit = 'LIMIT' in query_upper

            if has_limit:
                # User specified their own LIMIT, respect it but apply maximum
                df = conn.execute(query).fetchdf()
                applied_limit = len(df)
                is_truncated = False
            else:
                # Apply our default limit with offset for pagination
                limit = min(query_request.limit or 1000, 10000)  # Max 10k rows
                offset = query_request.offset or 0
                paginated_query = f"{query} LIMIT {limit} OFFSET {offset}"
                df = conn.execute(paginated_query).fetchdf()
                applied_limit = limit
                is_truncated = total_rows is not None and (offset + len(df)) < total_rows

            result = {
                "data": df.to_dict('records'),
                "columns": list(df.columns),
                "row_count": len(df),
                "total_rows": total_rows,
                "offset": query_request.offset or 0,
                "limit": applied_limit,
                "is_truncated": is_truncated
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
    """Ask a natural language question about the data using text-to-SQL conversion (Ollama first, OpenAI fallback)"""
    try:
        conn = get_db_connection()

        # Get database schema context
        if query.table_name:
            # Get schema for specific table
            tables_to_query = [query.table_name]
        else:
            # Get all tables
            tables_result = conn.execute("SHOW TABLES").fetchall()
            tables_to_query = [row[0] for row in tables_result]

        # Build schema context
        schema_context = "Database Schema:\n\n"
        for table in tables_to_query:
            columns = conn.execute(f"DESCRIBE {table}").fetchall()
            schema_context += f"Table: {table}\n"
            schema_context += "Columns:\n"
            for col in columns:
                schema_context += f"  - {col[0]} ({col[1]})\n"

            # Add sample data for better context (first 3 rows)
            sample = conn.execute(f"SELECT * FROM {table} LIMIT 3").fetchdf()
            schema_context += f"Sample data (first 3 rows):\n{sample.to_string()}\n\n"

        generated_sql = None
        llm_source = None

        # Try Ollama first (local, free, on-prem ready!)
        if check_ollama_available():
            try:
                print("Using Ollama for text-to-SQL conversion...")
                ensure_ollama_model("sqlcoder")

                sql_prompt = f"""### Task
Generate a SQL query to answer the question based on the database schema below.

### Database Schema
{schema_context}

### Question
{query.question}

### Instructions
- Write a valid DuckDB SQL SELECT query
- Include LIMIT 100 to prevent large result sets
- Return ONLY the SQL query with no explanations
- Use proper DuckDB syntax

### SQL Query
SELECT"""

                generated_sql_raw = query_ollama(sql_prompt, model="sqlcoder")

                print(f"Raw Ollama response: {generated_sql_raw}")

                if generated_sql_raw:
                    # Clean up the response - SQLCoder often returns extra text
                    generated_sql = generated_sql_raw.strip()

                    # Remove markdown code blocks
                    if "```" in generated_sql:
                        parts = generated_sql.split("```")
                        for part in parts:
                            if "SELECT" in part.upper() or "WITH" in part.upper():
                                generated_sql = part
                                break

                    # Remove "sql" or "SQL" prefix if present
                    if generated_sql.lower().startswith("sql"):
                        generated_sql = generated_sql[3:].strip()

                    # Add SELECT back if it was part of the prompt completion
                    if not generated_sql.upper().startswith("SELECT") and not generated_sql.upper().startswith("WITH"):
                        generated_sql = "SELECT " + generated_sql

                    # Remove trailing semicolon and whitespace
                    generated_sql = generated_sql.strip().rstrip(";").strip()

                    # Validate it looks like SQL
                    if generated_sql and (
                        "SELECT" in generated_sql.upper() or
                        "WITH" in generated_sql.upper()
                    ):
                        llm_source = "Ollama (SQLCoder)"
                        print(f"Cleaned SQL from Ollama: {generated_sql}")
                    else:
                        print(f"Ollama response doesn't look like valid SQL, falling back")
                        generated_sql = None

            except Exception as e:
                print(f"Ollama error: {e}, falling back to OpenAI...")
                generated_sql = None

        # Fallback to OpenAI if Ollama failed or unavailable
        if not generated_sql and OPENAI_API_KEY:
            try:
                print("Using OpenAI for text-to-SQL conversion...")
                import openai
                client = openai.OpenAI(api_key=OPENAI_API_KEY)

                system_prompt = """You are a SQL expert. Convert natural language questions into DuckDB SQL queries.
Rules:
1. Return ONLY the SQL query, no explanations
2. Use proper DuckDB syntax
3. Always include LIMIT 100 to prevent large result sets
4. Use appropriate JOINs if multiple tables are needed
5. For aggregations, use GROUP BY properly
6. Return SELECT queries only, no modifications"""

                response = client.chat.completions.create(
                    model="gpt-4",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f"{schema_context}\n\nQuestion: {query.question}\n\nGenerate SQL query:"}
                    ],
                    max_tokens=500,
                    temperature=0
                )

                generated_sql = response.choices[0].message.content.strip()
                # Remove markdown code blocks if present
                if generated_sql.startswith("```"):
                    generated_sql = generated_sql.split("```")[1]
                    if generated_sql.startswith("sql"):
                        generated_sql = generated_sql[3:]
                generated_sql = generated_sql.strip()
                llm_source = "OpenAI (GPT-4)"

            except Exception as e:
                print(f"OpenAI error: {e}")
                conn.close()
                return {
                    "answer": f"Both Ollama and OpenAI failed. Error: {str(e)}",
                    "error": str(e)
                }

        # If no LLM available
        if not generated_sql:
            conn.close()
            return {
                "answer": "No AI service available. Please configure either Ollama (local, free) or OpenAI API key.",
                "available_tables": tables_to_query
            }

        # Execute the generated SQL
        try:
            result_df = conn.execute(generated_sql).fetchdf()

            # Generate natural language answer
            answer_text = f"Based on the query results, "

            # Try to generate a better answer with LLM
            if check_ollama_available():
                answer_prompt = f"Question: {query.question}\n\nSQL Query: {generated_sql}\n\nResults:\n{result_df.to_string()}\n\nProvide a clear, concise answer to the question:"
                answer_text = query_ollama(answer_prompt, model="sqlcoder") or answer_text
            elif OPENAI_API_KEY:
                import openai
                client = openai.OpenAI(api_key=OPENAI_API_KEY)
                answer_response = client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a helpful data assistant. Provide a concise, natural language answer based on the query results."},
                        {"role": "user", "content": f"Question: {query.question}\n\nSQL Query: {generated_sql}\n\nResults:\n{result_df.to_string()}\n\nProvide a clear answer:"}
                    ],
                    max_tokens=300
                )
                answer_text = answer_response.choices[0].message.content

            conn.close()

            return {
                "answer": answer_text,
                "sql_query": generated_sql,
                "result_data": result_df.to_dict('records')[:10],  # Limit displayed results
                "row_count": len(result_df),
                "llm_source": llm_source
            }

        except Exception as sql_error:
            conn.close()
            return {
                "answer": f"I generated a SQL query but it failed to execute: {str(sql_error)}",
                "sql_query": generated_sql,
                "error": str(sql_error),
                "llm_source": llm_source
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
