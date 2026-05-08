# Script to incrementally synchronize data from PostgreSQL to ClickHouse using a scheduler.
# Usage: python sync_to_clickhouse.py
from sqlalchemy import create_engine
import pandas as pd
import clickhouse_connect
import time
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import os

SYNC_FILE = "sync_state.txt"

pg_engine = create_engine(
    "postgresql+psycopg2://postgres:root@localhost/testdb"
)

clickhouse_client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='root'
)

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def get_last_sync_time():
    if not os.path.exists(SYNC_FILE):
        return "1970-01-01 00:00:00"
    with open(SYNC_FILE, "r") as f:
        return f.read().strip()

def save_sync_time(sync_time):
    with open(SYNC_FILE, "w") as f:
        f.write(sync_time)

def sync_data():
    try:
        last_sync = get_last_sync_time()

        log("=" * 60)
        log(f"Sync started. Last sync timestamp: {last_sync}")

        query = f"""
        SELECT
            o.id AS order_id,
            c.name AS customer_name,
            c.city,
            p.name AS product_name,
            p.category,
            oi.quantity,
            oi.price::float,
            o.created_at,
            now() as synced_at
        FROM orders o
        JOIN customers c
            ON c.id = o.customer_id
        JOIN order_items oi
            ON oi.order_id = o.id
        JOIN products p
            ON p.id = oi.product_id
        WHERE
            o.updated_at > '{last_sync}'
            OR c.updated_at > '{last_sync}'
            OR p.updated_at > '{last_sync}'
            OR oi.updated_at > '{last_sync}'
        """

        start = time.perf_counter()
        df = pd.read_sql(query, pg_engine)
        fetch_end = time.perf_counter()

        log(f"Rows fetched: {len(df)} (Fetch time: {(fetch_end - start):.4f}s)")

        if len(df) > 0:
            clickhouse_client.insert_df(
                "poc.order_analytics",
                df
            )
            log(f"Successfully inserted {len(df)} rows into ClickHouse")

        end = time.perf_counter()
        
        new_sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_sync_time(new_sync_time)

        log(f"Sync completed. Total time: {(end - start):.4f}s. New sync timestamp: {new_sync_time}")
        log("=" * 60)

    except Exception as e:
        log(f"CRITICAL ERROR during sync: {str(e)}")

scheduler = BlockingScheduler()

scheduler.add_job(
    sync_data,
    'interval',
    seconds=30
)

log("Starting incremental sync scheduler (Interval: 30s)...")

sync_data()

try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    log("Scheduler stopped.")