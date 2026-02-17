import os
import snowflake.connector
import pandas as pd
from config import SNOWFLAKE_CONFIG

try:
    # ---------- GET PROJECT ROOT PATH ----------
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(BASE_DIR, "data", "orders.csv")

    print("📂 Reading file from:", file_path)

    # ---------- CHECK FILE EXISTS ----------
    if not os.path.exists(file_path):
        raise FileNotFoundError("❌ orders.csv file not found!")

    # ---------- CHECK FILE SIZE ----------
    if os.path.getsize(file_path) == 0:
        raise ValueError("❌ orders.csv file is empty!")

    # ---------- LOAD CSV ----------
    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError("❌ CSV contains no records!")

    print(f"📄 Found {len(df)} records in CSV")

    # ---------- CONNECT TO SNOWFLAKE ----------
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
        role=SNOWFLAKE_CONFIG["role"]
    )

    cursor = conn.cursor()

    print("✅ Connected to Snowflake successfully")
    print("Database :", SNOWFLAKE_CONFIG['database'])
    print("Schema   :", SNOWFLAKE_CONFIG['schema'])

    # ---------- SET SESSION CONTEXT ----------
    cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")
    cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
    cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")

    # ---------- INSERT QUERY ----------
    insert_query = """
    INSERT INTO ORDERS_RAW
    (ORDER_ID, CUSTOMER_NAME, PRODUCT_NAME, QUANTITY, PRICE, ORDER_DATE)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    # ---------- INSERT DATA ----------
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            int(row["ORDER_ID"]),
            str(row["CUSTOMER_NAME"]),
            str(row["PRODUCT_NAME"]),
            int(row["QUANTITY"]),
            float(row["PRICE"]),
            str(row["ORDER_DATE"])
        ))

    conn.commit()
    print("🎉 Data inserted successfully into Snowflake!")

except Exception as e:
    print("❌ Error occurred:", e)

finally:
    try:
        cursor.close()
        conn.close()
        print("🔒 Connection closed")
    except:
        pass
