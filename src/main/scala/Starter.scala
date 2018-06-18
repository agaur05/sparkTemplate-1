import org.apache.spark.sql.{DataFrame, SparkSession}

object Starter {

  def main(args: Array[String]): Unit = {
    // Where we want the data to reside (src/main/resources)
    val dataFilePath = getClass.getResource("/database.csv").getPath

    // Setup SparkSession
    val spark = SparkSession.builder
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    // Load data
    val df = loadData(spark, dataFilePath)

    // Print sample of data
    df.show()

  }

  /**
    * Load the Pantheon data into a dataframe
    *
    * @param spark the spark session
    * @param path the path where the data resides
    * @return the dataframe
    */
  def loadData(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header", true).csv(path)
  }

}
