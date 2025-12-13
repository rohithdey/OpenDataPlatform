FROM apache/airflow:2.8.1-python3.11

USER airflow

RUN pip install --no-cache-dir \
    duckdb==0.9.2 \
    pandas==2.1.4 \
    requests==2.31.0 \
    pyarrow==15.0.0 \
    deltalake>=0.14.0 \
    yfinance>=0.2.43 \
    lxml==5.1.0 \
    html5lib \
    beautifulsoup4 \
    fredapi>=0.5.1
