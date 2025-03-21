from base.spark_job import SparkJob
from schemas import *


class Save(SparkJob):

    def run(self, config):
        try:
            df = self.spark.read.parquet(
                config["input_path"],
                schema=eval(config["schema_name"])).select(config["fields"])

            df.write \
                .format("jdbc") \
                .option("url", config["db"]["url"]) \
                .option("dbtable", config["table_name"]) \
                .option("user", config["db"]["properties"]["user"]) \
                .option("password", config["db"]["properties"]["password"]) \
                .option("driver", config["db"]["properties"]["driver"]) \
                .mode("append") \
                .save()

            print("Data successfully saved to tb_address in PostgreSQL!")

        except Exception as e:
            raise Exception(f"Error processing data: {e}")