from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
data = [(1, "Alice"), (2, "Bob")]
columns = ["id", "name"]
df = spark.createDataFrame(data, columns)
df.show()
spark.stop()
