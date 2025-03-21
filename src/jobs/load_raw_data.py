from pyspark.sql.functions import unix_timestamp, monotonically_increasing_id, concat_ws
from lib.utils import optimize_partitioning
from base.spark_job import SparkJob


class LoadRawData(SparkJob):

    def run(self, config):
        input_path = config["input_path"]
        output_path = config["output_path"]

        try:
            df = self.spark.read.csv(input_path, header=True, inferSchema=True)
            num_cores = self.spark.sparkContext.defaultParallelism
            num_partitions = df.rdd.getNumPartitions()

            # Optimize the number of partitions for better performance
            df_optimized = optimize_partitioning(df, num_cores, num_partitions)
            df_final = df_optimized \
                .toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns]) \
                .withColumn("runtime_id", concat_ws("_", unix_timestamp(), monotonically_increasing_id())) \
                .withColumnRenamed("account_created_at4", "account_created_at")

            # Save as Parquet
            df_final.write.mode("overwrite").parquet(output_path)
            print(f"Data successfully written to {output_path}")

        except Exception as e:
            raise Exception(f"Error processing data: {e}")