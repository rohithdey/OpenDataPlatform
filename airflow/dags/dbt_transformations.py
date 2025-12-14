"""
DBT Transformations Pipeline DAG
Runs DBT models to transform raw data into analytics-ready marts.

Flow:
1. Staging models (clean source data)
2. Intermediate models (business logic)
3. Mart models (final analytics tables)
4. Run tests to validate data quality
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os

# Configuration
DBT_PROJECT_DIR = '/opt/airflow/dbt'
DBT_PROFILES_DIR = '/opt/airflow/dbt'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Common DBT command prefix
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"


def check_dbt_installed(**context):
    """Verify DBT is installed and configured."""
    import subprocess
    result = subprocess.run(['dbt', '--version'], capture_output=True, text=True)
    print(f"DBT Version:\n{result.stdout}")

    # Check if project exists
    if not os.path.exists(f"{DBT_PROJECT_DIR}/dbt_project.yml"):
        raise FileNotFoundError(f"DBT project not found at {DBT_PROJECT_DIR}")

    print(f"DBT project found at {DBT_PROJECT_DIR}")
    return True


with DAG(
    'dbt_transformations',
    default_args=default_args,
    description='Run DBT transformations on finance data',
    schedule_interval='0 10 * * *',  # Daily at 10 AM (after data pipelines)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'transformations', 'analytics'],
) as dag:

    dag.doc_md = """
    ## DBT Transformations Pipeline

    Runs DBT models to transform raw data into analytics-ready tables.

    ### Model Layers:
    - **Staging**: Cleaned source data (stg_*)
    - **Intermediate**: Business logic transformations (int_*)
    - **Marts**: Final analytics tables (mart_*)

    ### Data Sources Transformed:
    - Stock prices (Yahoo Finance)
    - Economic indicators (FRED)
    - SEC filings (EDGAR)
    - Cryptocurrency (CoinGecko)

    ### Trigger manually with:
    ```json
    {"full_refresh": true}  // Force full rebuild
    ```
    """

    # Task: Check DBT installation
    check_dbt = PythonOperator(
        task_id='check_dbt_installed',
        python_callable=check_dbt_installed,
    )

    # Task: Run dbt deps (install packages)
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f"{DBT_CMD} deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Task: Run staging models
    dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f"{DBT_CMD} run --select staging --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Task: Run intermediate models
    dbt_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=f"{DBT_CMD} run --select intermediate --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Task: Run mart models
    dbt_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f"{DBT_CMD} run --select marts --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Task: Run tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"{DBT_CMD} test --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Task: Generate docs
    dbt_docs = BashOperator(
        task_id='dbt_generate_docs',
        bash_command=f"{DBT_CMD} docs generate --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Define task dependencies
    check_dbt >> dbt_deps >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test >> dbt_docs


# Additional DAG for on-demand full refresh
with DAG(
    'dbt_full_refresh',
    default_args=default_args,
    description='Full refresh of all DBT models',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'transformations', 'manual'],
) as dag_refresh:

    dag_refresh.doc_md = """
    ## DBT Full Refresh

    Manually triggered DAG to rebuild all DBT models from scratch.
    Use when schema changes or data needs to be completely reprocessed.
    """

    dbt_full_refresh = BashOperator(
        task_id='dbt_full_refresh',
        bash_command=f"{DBT_CMD} run --full-refresh --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test_after_refresh = BashOperator(
        task_id='dbt_test',
        bash_command=f"{DBT_CMD} test --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_full_refresh >> dbt_test_after_refresh


# DAG for running specific models
with DAG(
    'dbt_run_model',
    default_args=default_args,
    description='Run specific DBT model(s)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'transformations', 'manual'],
    params={
        'model_selector': 'marts',
    }
) as dag_model:

    dag_model.doc_md = """
    ## Run Specific DBT Model

    Trigger with config to run specific models:
    ```json
    {"model_selector": "mart_stock_summary"}
    {"model_selector": "staging"}
    {"model_selector": "tag:crypto"}
    ```
    """

    run_selected = BashOperator(
        task_id='dbt_run_selected',
        bash_command=f"{DBT_CMD} run --select {{{{ dag_run.conf.get('model_selector', 'marts') }}}} --profiles-dir {DBT_PROFILES_DIR}",
    )
