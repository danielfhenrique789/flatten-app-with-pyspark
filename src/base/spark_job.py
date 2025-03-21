from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class SparkJob(ABC):
    """Base class for all Spark jobs"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def run(self, config: dict):
        """This method must be implemented by all subclasses"""
        pass