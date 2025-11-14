import org.apache.spark.sql.{SaveMode, SparkSession}

object AppendToParquet {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Append to Parquet File")
      .master("local[*]")
      .getOrCreate()

    // Read the existing Parquet file from HDFS
    val existingDF = spark.read.parquet("hdfs://namenode:9000/data/users.parquet")

    // Create a DataFrame from the new JSON data
    val newData = Seq(
      ("Michael", null),           // New row with just "name"
      ("Andy", Some(30)),          // New row with "name" and "age"
      ("Justin", Some(19))         // New row with "name" and "age"
    )
    import spark.implicits._
    val newDF = newData.toDF("name", "age")

    // Union the existing DataFrame and the new DataFrame
    val combinedDF = existingDF.union(newDF)

    // Write the combined DataFrame back to Parquet in HDFS, append the data
    combinedDF.write
      .mode(SaveMode.Append)  // Append mode to add data to the existing Parquet file
      .parquet("hdfs://namenode:9000/data/users.parquet")

    // Stop the Spark session
    spark.stop()
  }
}
