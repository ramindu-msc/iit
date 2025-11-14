import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateParquetFile {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Create Parquet File")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Define sample data
    val usersData = Seq(
      (1, "Alice", 29, "USA"),
      (2, "Bob", 35, "UK"),
      (3, "Charlie", 30, "Canada"),
      (4, "Diana", 27, "Australia"),
      (5, "Ethan", 33, "India")
    )

    // Create a DataFrame from the data
    val usersDF = usersData.toDF("id", "name", "age", "country")

   println("Creating Spark DataFrame...")
   usersDF.show()

   println("Writing DataFrame as Parquet...")
   try {
     usersDF.write.mode(SaveMode.Overwrite).parquet("hdfs://namenode:9000/data/users.parquet")
     println("Parquet file written successfully!")
   } catch {
     case e: Exception => println(s"Error writing Parquet file: ${e.getMessage}")
   }

    spark.stop()
  }
}