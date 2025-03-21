import os
import sys
from pyspark.sql import SparkSession
from lib.auto_load import get_job_class
from lib.main_utils import get_job_name
from lib.config_manager import load_config, get_config

# Load configuration
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/config.json')


def main():
    job_name = get_job_name(sys.argv)
    config = get_config(load_config(CONFIG_PATH), job_name)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(job_name) \
        .master("local[*]") \
        .getOrCreate()

    # Dynamically instantiate and run job
    job_class = get_job_class(job_name)
    job_instance = job_class(spark)
    job_instance.run(config)

    spark.stop()


if __name__ == "__main__":
    main()
