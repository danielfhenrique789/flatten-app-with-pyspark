from pyspark.sql.types import StructType, StructField, IntegerType, StringType

address_schema = StructType([
    StructField("runtime_id", StringType(), True),  # Foreign Key (references tb_client)
    StructField("street", StringType(), True),       # Street address
    StructField("city", StringType(), True),         # City name
    StructField("post_code", StringType(), True),    # Postal code (kept as STRING for consistency)
    StructField("country", StringType(), True)       # Country name
])