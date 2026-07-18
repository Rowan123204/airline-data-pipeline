from pyspark.sql import SparkSession

# Initialize the SparkSession
spark = (SparkSession.builder
         .appName("MyPySparkApp")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

# Verify the session is running
print(spark.version)
