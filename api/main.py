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
