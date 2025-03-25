from base.spark_job import SparkJob
from schemas import *


class Test(SparkJob):

    def run(self, config):
        try:
            df = self.spark.read.csv(config["input_path"], inferSchema=True).show()
            #df = self.spark.createDataFrame([("Alice", 1), ("Bob", 2)], ["name", "id"])

            #df.write.mode("overwrite").parquet("s3a://spark-bucket/output")

            print("✅ WRITE SUCCESSFUL")

        except Exception as e:
            raise Exception(f"Error processing data: {e}")

