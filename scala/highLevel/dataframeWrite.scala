package highLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dataframeWrite {
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

    //df.show()

    val df2 = df
      .withColumn("isim",trim(initcap($"isim")))
      .withColumn("cinsiyet",when($"cinsiyet".isNull,"U").otherwise($"cinsiyet"))
      .withColumn("sehir",
        when($"sehir".isNull,"BILINMIYOR")
          .otherwise($"sehir"))
      .withColumn("sehir",trim(upper($"sehir")))

    df2.show(truncate = false)

    df2.coalesce(1)
      .write
      .mode("Overwrite")
      .option("sep",",")
      .option("header","true")
      .csv("/home/basayev/Downloads/exampleWrite")
  }
}
