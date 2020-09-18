package highLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object stringOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("StringOps")
      .config("spark.driver.memory","4g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("/home/basayev/Downloads/sparkDatas/simple_dirty_data.csv")

    //df.show(10)

    val df2 = df.select("meslek","sehir")
      .withColumn("meslek_sehir",concat(col("meslek"),lit(" - "),col("sehir")))
      .show(truncate = false)

  }
}
