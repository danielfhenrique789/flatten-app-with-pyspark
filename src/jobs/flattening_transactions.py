from pyspark.sql.functions import lit, substring, col,to_timestamp, from_json, regexp_replace, explode, split, concat, concat_ws, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from base.spark_job import SparkJob
from lib.utils import optimize_partitioning
from lib.utils import optimize_partitioning, compare_schemas
from schemas.transaction import transaction_schema
import traceback


class FlatteningTransactions(SparkJob):
    def run(self, config):
        print("✅ Spark Job Completed Successfully")

        input_path = config["input_path"]
        output_path = config["output_path"]

        try:
            df = self.spark.read.parquet(input_path, inferSchema=True).select(col("runtime_id"),col("transactions"))
            num_cores = self.spark.sparkContext.defaultParallelism
            num_partitions = df.rdd.getNumPartitions()

            # Optimize the number of partitions for better performance
            df_optimized = optimize_partitioning(df, num_cores, num_partitions)

            # Fixing Json
            df_fixed = self._fix_json(df_optimized)

            array_schema = ArrayType(StringType())

            df_split_transactions = df_fixed.withColumn("transactions", from_json(col("transactions"), array_schema)) \
                .withColumn("transaction", explode(col("transactions"))).drop("transactions")

            # Split json string in two columns
            df_split_columns = self._split_transaction_columns(df_split_transactions, ",\"tt\":")

            # Define Schema to Ensure `post_code` is Treated as STRING
            transaction_json_schema = StructType([
                StructField("amount", StringType(), True),
                StructField("transaction_date", StringType(), True)
            ])

            tt_schema = StructType([
                StructField("t", StringType(), True),
                StructField("from", StringType(), True),
                StructField("to", StringType(), True)
            ])

            # Convert JSON String to Structured Columns Using Schema
            column_name = "transaction"
            column_name_tt = "tt"
            df_parsed = df_split_columns.withColumn(column_name, from_json(col(column_name), transaction_json_schema)) \
                .withColumn(column_name_tt, from_json(col(column_name_tt), tt_schema))

            # Extract JSON Fields Dynamically
            df_final = self._format_final_df(df_parsed, column_name, column_name_tt)

            # Check final Schema
            #raise Exception(f"desgracaaa: {df_final.schema}")
            compare_schemas(df_final.schema, transaction_schema)

            # Save as Parquet
            df_final.write.mode("overwrite").parquet(output_path)
            print(f"Data successfully written to {output_path}")

        except Exception as e:
            raise Exception(f"Error processing data:\n{e}\n{traceback.format_exc()}")

    def _format_final_df(self, df, column_name, column_name_tt):
        return df.select(
                col("runtime_id"),
                col(f"{column_name_tt}.from").alias("client_from"),
                col(f"{column_name_tt}.to").alias("client_to"),
                col(f"{column_name}.amount").alias("amount"),
                col(f"{column_name}.transaction_date").alias("transaction_date"),
                col(f"{column_name_tt}.t").alias("transaction_type")
            )\
            .withColumn("transaction_date", concat_ws(
                " ",
                concat_ws("-",
                    substring(col("transaction_date"), 1, 4),  # Extract year
                    substring(col("transaction_date"), 6, 2),  # Extract month
                    substring(col("transaction_date"), 9, 2)   # Extract day
                ),
                substring(col("transaction_date"), 11, 20)  # Extract time part
            )) \
            .withColumn("transaction_date", to_timestamp("transaction_date", "yyyy-MM-dd HH:mm:ss.SSSSSS"))


    def _fix_json(self, df):
        return df.withColumn("transactions", regexp_replace(col("transactions"), r"(?i)'(?:(?i)transaction[a-zA-Z_]*)'", "'transaction_date'")) \
                .withColumn("transactions", regexp_replace(col("transactions"), "'transaction date'", "'transaction_date'")) \
                .withColumn("transactions", regexp_replace(col("transactions"), " ", "")) \
                .withColumn("transactions", regexp_replace(col("transactions"), r"\{'amount'", r"\"\{'amount'")) \
                .withColumn("transactions", regexp_replace(col("transactions"), r"\}\},\"\{", r"\}\}\",\"\{")) \
                .withColumn("transactions", regexp_replace(col("transactions"), r"\}\]", r"\}\"\]"))


    def _split_transaction_columns(self, df, delimiter):
        return df.withColumn("transaction", regexp_replace(col("transaction"), "'", "\"")) \
            .withColumn("transaction_data", split(col("transaction"), delimiter)[0]) \
            .withColumn("tt", split(col("transaction"), delimiter)[1]) \
            .withColumn("transaction", concat(col("transaction_data"), lit("}"))) \
            .withColumn("tt", regexp_replace(col("tt"), "\}\}", "\}")) \
            .withColumn("runtime_id_transaction", concat_ws("_", col("runtime_id"), monotonically_increasing_id())) \
            .drop("transaction_data")