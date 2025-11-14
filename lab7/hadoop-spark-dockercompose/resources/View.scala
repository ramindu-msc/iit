import org.apache.spark.sql.SparkSession

// Initialize Spark Session
val spark = SparkSession.builder()
  .appName("View Parquet Data")
  .master("local[*]")  // Adjust the master to match your environment (e.g., local, yarn)
  .getOrCreate()

// Read the Parquet file from HDFS
val df = spark.read.parquet("hdfs://namenode:9000/data/users.parquet")

// Show the contents of the DataFrame
df.show()