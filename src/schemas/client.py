from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

client_schema = StructType([
    StructField("runtime_id", StringType(), False),       # Unique identifier
    StructField("names", StringType(), False),            # Full name,
    StructField("mail", StringType(), False),
    StructField("account_created_at", TimestampType(), True)  # Timestamp of account creation
])

