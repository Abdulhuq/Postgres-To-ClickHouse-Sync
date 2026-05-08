# Utility script to create or delete required tables in both PostgreSQL and ClickHouse.
# Usage: python manage_tables.py --action [create|delete]
import psycopg2
import argparse
import clickhouse_connect
from datetime import datetime

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# PostgreSQL connection
pg_conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="testdb",
    user="postgres",
    password="root"
)
pg_conn.autocommit = True
pg_cur = pg_conn.cursor()

# ClickHouse connection
ch_client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='root'
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--action",
    choices=["create", "delete"],
    required=True
)
args = parser.parse_args()

if args.action == "create":
    log("Creating PostgreSQL tables...")
    
    pg_cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        id SERIAL PRIMARY KEY,
        name TEXT,
        city TEXT,
        updated_at TIMESTAMP DEFAULT now()
    );
    """)

    pg_cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name TEXT,
        category TEXT,
        updated_at TIMESTAMP DEFAULT now()
    );
    """)

    pg_cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        customer_id INT REFERENCES customers(id),
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now()
    );
    """)

    pg_cur.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id INT REFERENCES orders(id),
        product_id INT REFERENCES products(id),
        quantity INT,
        price NUMERIC,
        updated_at TIMESTAMP DEFAULT now()
    );
    """)
    log("PostgreSQL tables created successfully.")

    log("Creating ClickHouse database and tables...")
    ch_client.command("CREATE DATABASE IF NOT EXISTS poc")
    
    ch_client.command("""
    CREATE TABLE IF NOT EXISTS poc.order_analytics
    (
        order_id UInt32,
        customer_name String,
        city String,
        product_name String,
        category String,
        quantity UInt32,
        price Float64,
        created_at DateTime,
        synced_at DateTime
    )
    ENGINE = ReplacingMergeTree
    ORDER BY (order_id, created_at);
    """)
    log("ClickHouse tables created successfully.")

elif args.action == "delete":
    log("Dropping PostgreSQL tables...")
    pg_cur.execute("DROP TABLE IF EXISTS order_items CASCADE;")
    pg_cur.execute("DROP TABLE IF EXISTS orders CASCADE;")
    pg_cur.execute("DROP TABLE IF EXISTS products CASCADE;")
    pg_cur.execute("DROP TABLE IF EXISTS customers CASCADE;")
    log("PostgreSQL tables dropped successfully.")

    log("Dropping ClickHouse tables...")
    ch_client.command("DROP TABLE IF EXISTS poc.order_analytics")
    log("ClickHouse tables dropped successfully.")

pg_cur.close()
pg_conn.close()