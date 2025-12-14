# CloudBeaver DuckDB Setup Guide

## First-Time Setup (One-time only)

### Step 1: Access CloudBeaver
Open http://localhost:8978 in your browser

### Step 2: Create Admin Account
1. Click **"Configure"** on the welcome screen
2. Set **Server Name**: `OpenDataPlatform`
3. Set **Admin Credentials**:
   - Username: `admin`
   - Password: `admin` (or your choice)
4. Click **"Finish"**

### Step 3: Add DuckDB Connection
1. Click the **"+"** button in the left sidebar (or go to **Connection > New Connection**)
2. Search for **"DuckDB"** in the driver list
3. Select **DuckDB** driver
4. Configure the connection:

   | Field | Value |
   |-------|-------|
   | **Connection Name** | `OpenDataPlatform DuckDB` |
   | **Database/File Path** | `/opt/data/warehouse.duckdb` |
   | **Read Only** | Uncheck if you want to write |

5. Click **"Test Connection"** to verify
6. Click **"Create"**

### Step 4: Browse Your Data
1. Expand the connection in the left sidebar
2. Navigate to: `main` schema > `Tables`
3. You should see tables like:
   - `stock_prices`
   - `fred_economic`
   - `sec_filings`
   - `crypto_prices`

---

## Quick SQL Queries to Try

Once connected, click **"SQL Editor"** and try:

```sql
-- List all tables
SHOW TABLES;

-- View stock data
SELECT * FROM stock_prices LIMIT 100;

-- Stock summary by symbol
SELECT
    symbol,
    COUNT(*) as rows,
    MIN(date) as first_date,
    MAX(date) as last_date,
    AVG(close) as avg_price
FROM stock_prices
GROUP BY symbol;

-- View Delta Lake tables directly
SELECT * FROM delta_scan('/opt/data/delta/stock_prices') LIMIT 10;

-- Time travel query (if Delta Lake extension is loaded)
INSTALL delta;
LOAD delta;
SELECT * FROM delta_scan('/opt/data/delta/stock_prices', version=0) LIMIT 10;
```

---

## Troubleshooting

### "Database file not found"
- Make sure the path is exactly: `/opt/data/warehouse.duckdb`
- Ensure data pipelines have run at least once

### "Connection refused"
- Check that containers are running: `docker-compose ps`
- Restart CloudBeaver: `docker-compose restart duckdb-ide`

### "Read-only database"
- Uncheck "Read Only" in connection settings
- Note: Only one connection can write at a time
