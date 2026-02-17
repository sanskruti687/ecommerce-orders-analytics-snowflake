import snowflake.connector
import pandas as pd

conn = snowflake.connector.connect(
    user="YOUR_USER",
    password="YOUR_PASSWORD",
    account="YOUR_ACCOUNT",
    warehouse="COMPUTE_WH",
    database="SHOP_DB",
    schema="RAW"
)

cursor = conn.cursor()

df = df = pd.read_csv("../data/orders.csv")


for _, row in df.iterrows():
    query = f"""
    INSERT INTO ORDERS_RAW
    VALUES ({row.ORDER_ID}, '{row.CUSTOMER_NAME}',
            '{row.PRODUCT_NAME}', {row.QUANTITY},
            {row.PRICE}, '{row.ORDER_DATE}');
    """
    cursor.execute(query)

print("Data inserted successfully")

cursor.close()
conn.close()
