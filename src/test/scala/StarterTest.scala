import org.scalatest.FlatSpec

class StarterTest extends FlatSpec with SparkEnvironment {

  "starter" must "load data into dataframe" in {
    val dataFilePath =  getClass.getResource("/database.csv").getPath
    val df = Starter.loadData(spark, dataFilePath)
    assert(df.count == 11341)
  }

}
