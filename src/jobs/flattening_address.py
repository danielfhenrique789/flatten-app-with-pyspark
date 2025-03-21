from pyspark.sql.functions import when,  col, from_json, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType
from base.spark_job import SparkJob
from lib.utils import optimize_partitioning, compare_schemas
from schemas.address import address_schema
import traceback


class FlatteningAddress(SparkJob):
    def run(self, config):

        input_path = config["input_path"]
        output_path = config["output_path"]

        try:
            df = self.spark.read.parquet(input_path, inferSchema=True).select(col("runtime_id"),col("address"))
            num_cores = self.spark.sparkContext.defaultParallelism
            num_partitions = df.rdd.getNumPartitions()

            # Optimize the number of partitions for better performance
            df_optimized = optimize_partitioning(df, num_cores, num_partitions)

            # Fixing Json
            df_fixed = self._fix_json(df_optimized)

            # Define Schema to Ensure `post_code` is Treated as STRING
            address_json_schema = StructType([
                StructField("street", StringType(), True),
                StructField("city", StringType(), True),
                StructField("post_code", StringType(), True),  # Now always STRING
                StructField("country", StringType(), True)
            ])

            # Convert JSON String to Structured Columns Using Schema
            df_parsed = df_fixed.withColumn("address", from_json(col("address"), address_json_schema))

            # Extract JSON Fields Dynamically
            df_final = self._format_final_df(df_parsed)

            # Check final Schema
            compare_schemas(df_final.schema, address_schema)

            # Save as Parquet
            df_final.write.mode("overwrite").parquet(output_path)
            print(f"Data successfully written to {output_path}")

        except Exception as e:
            raise Exception(f"Error processing data:\n{e}\n{traceback.format_exc()}")


    def _fix_json(self, df):
        return df.withColumn("address", regexp_replace(col("address"), "'post code'", "'post_code'")) \
            .withColumn("address", when(col("address") == "", None).otherwise(col("address"))) \
            .withColumn("address", regexp_replace(col("address"), " ", "")) \
            .withColumn("address", regexp_replace(col("address"), r"\{'address'\:", "")) \
            .withColumn("address", regexp_replace(col("address"), r"\}\}", r"\}")) \
            .withColumn("address", regexp_replace(col("address"), "'", "\"")) \
            .withColumn("address", regexp_replace(col("address"), r'"post_code":(\d+[-]?\d*)', r'"post_code":"\1"'))


    def _format_final_df(self, df):
        return df.select(
            col("runtime_id"),
            col("address.street"),
            col("address.city").alias("city"),
            col("address.post_code").alias("post_code"),  # Now always STRING
            col("address.country").alias("country")
        )