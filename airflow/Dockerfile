FROM apache/airflow:2.8.1-python3.11

USER airflow

RUN pip install --no-cache-dir \
    duckdb==0.9.2 \
    pandas==2.1.4 \
    requests==2.31.0 \
    pyarrow==15.0.0 \
    yfinance==0.2.33 \
    pyiceberg[duckdb]==0.6.0
