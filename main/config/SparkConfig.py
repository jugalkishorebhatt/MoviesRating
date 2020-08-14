from pyspark.sql import SparkSession

class SparkConfig:
    pass

    def __init__(self):
        pass
    
    def __configurations(self):
        return SparkSession.builder.appName('Senscity').getOrCreate();