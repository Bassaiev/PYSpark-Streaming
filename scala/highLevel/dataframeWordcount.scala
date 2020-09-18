package highLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object dataframeWordcount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("dfWordcount")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    import spark.implicits._

    val hikayeDs = spark.read.textFile("/home/basayev/Downloads/hikaye.txt")
    val hikayeDf =hikayeDs.toDF("value")
    //hikayeDf.show(20,truncate = false)

    val kelimeler = hikayeDs.flatMap(x => x.split(" "))
    //println(kelimeler.count())

    import org.apache.spark.sql.functions.{count}

    kelimeler.groupBy("value")
      .agg(count("value").as("kelimeSayisi"))
      .orderBy($"kelimeSayisi".desc)
      .show(10)

  }
}
