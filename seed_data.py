# Script to generate and insert mock data into PostgreSQL for testing purposes.
# Usage: python seed_data.py
from faker import Faker
import psycopg2
import psycopg2.extras
import random
import time
from datetime import datetime

fake = Faker()

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="testdb",
    user="postgres",
    password="root"
)

cur = conn.cursor()

TOTAL = 100000
BATCH_SIZE = 5000

log("Starting periodic data insertion (Optimized)...")

try:
    while True:
        start = time.perf_counter()
        
        log(f"--- Starting new batch of {TOTAL} records ---")
        
        log("Inserting customers...")
        for i in range(0, TOTAL, BATCH_SIZE):
            customers = [(fake.name(), fake.city()) for _ in range(min(BATCH_SIZE, TOTAL - i))]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO customers (name, city) VALUES %s",
                customers
            )
            log(f"  > Inserted {min(i + BATCH_SIZE, TOTAL)}/{TOTAL} customers")
        conn.commit()

        log("Inserting products...")
        for i in range(0, TOTAL, BATCH_SIZE):
            products = [(fake.word(), random.choice(["tech", "fashion", "food", "sports"])) 
                       for _ in range(min(BATCH_SIZE, TOTAL - i))]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO products (name, category) VALUES %s",
                products
            )
            log(f"  > Inserted {min(i + BATCH_SIZE, TOTAL)}/{TOTAL} products")
        conn.commit()

        log("Inserting orders and order_items...")

        ORDER_BATCH = 1000
        for i in range(0, TOTAL, ORDER_BATCH):
            current_batch_size = min(ORDER_BATCH, TOTAL - i)
            
            # Get latest ID range for customers and products to pick from
            # This is slightly slow but ensures we pick valid IDs if the table is growing
            cur.execute("SELECT MAX(id) FROM customers")
            max_customer_id = cur.fetchone()[0] or 1
            cur.execute("SELECT MAX(id) FROM products")
            max_product_id = cur.fetchone()[0] or 1

            orders_data = [(random.randint(1, max_customer_id),) for _ in range(current_batch_size)]
            
            # Insert orders and get their IDs
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO orders (customer_id) VALUES %s RETURNING id",
                orders_data
            )
            order_ids = [r[0] for r in cur.fetchall()]
            
            items_data = []
            for o_id in order_ids:
                for _ in range(random.randint(1, 5)):
                    items_data.append((
                        o_id,
                        random.randint(1, max_product_id),
                        random.randint(1, 10),
                        round(random.uniform(10, 500), 2)
                    ))
            
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES %s",
                items_data
            )
            log(f"  > Inserted {min(i + ORDER_BATCH, TOTAL)}/{TOTAL} orders and their items")
            conn.commit()

        end = time.perf_counter()
        log(f"Batch completed in {(end - start):.2f} seconds.")
        log("Waiting 5 seconds before next batch...")
        time.sleep(5)

except KeyboardInterrupt:
    log("Stopping data insertion...")
except Exception as e:
    log(f"CRITICAL ERROR: {str(e)}")
finally:
    cur.close()
    conn.close()
    log("Database connection closed.")