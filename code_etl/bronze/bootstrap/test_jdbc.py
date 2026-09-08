"""Quick JDBC test to check source data readability."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("jdbc-test").getOrCreate()

tables = [
    ("card_crm.card_txn", "txn_id"),
    ("digital_banking.device", "device_id"),
    ("digital_banking.online_transaction", "transaction_id"),
    ("digital_banking.support_ticket", "ticket_id"),
    ("digital_banking.location", "location_id"),
]

for schema_table, pk in tables:
    try:
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/banking_db") \
            .option("dbtable", f"(SELECT {pk} FROM {schema_table}) t") \
            .option("user", "banking_admin") \
            .option("password", "BankingAdmin123") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        count = df.count()
        print(f"OK: {schema_table} -> {count} rows")
    except Exception as e:
        print(f"FAIL: {schema_table} -> {e}")

spark.stop()
