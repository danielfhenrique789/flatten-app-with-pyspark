from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType

transaction_schema = StructType([
    StructField('runtime_id', StringType(), True),
    StructField('client_from', StringType(), True),
    StructField('client_to', StringType(), True),
    StructField('amount', StringType(), True),
    StructField('transaction_date', TimestampType(), True),
    StructField('transaction_type', StringType(), True)
])

