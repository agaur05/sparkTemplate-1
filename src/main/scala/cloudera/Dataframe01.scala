package cloudera

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object Dataframe01 {

  def main(args: Array[String]): Unit = {

    val peopleFilePath = getClass.getResource("/people-no-pcode.csv").getPath
    val pcodeFilePath = getClass.getResource("/zcodes.csv").getPath


    //setup Spark Session
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    val pDF = spark.read.option("header", true).csv(peopleFilePath)
    val pcodeDF = spark.read.option("header", true).csv(pcodeFilePath)

    pDF.join(pcodeDF, "pcode").show()


  }

}
