package highLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

object dateOps {
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
      .option("sep",";")
      .option("inferSchema","true")
      .load("/home/basayev/Downloads/sparkDatas/OnlineRetail2")
      .select("InvoiceDate").distinct()

    //df.show(5)

    // yyyy-MM-dd HH:mm:ss

    val mevcutFormat = "dd.MM.yyyy HH:mm"
    val formatTR = "dd/MM/yyyy HH:mm:ss"
    val formatENG = "MM-dd-yyyy HH:mm:ss"

    val df2 = df.withColumn("InvoiceDate" , F.trim($"InvoiceDate"))
      .withColumn("Normal",F.to_date($"InvoiceDate",mevcutFormat))
        .withColumn("StandartTS",F.to_timestamp($"InvoiceDate",mevcutFormat))
        .withColumn("UnixTS",F.unix_timestamp($"StandartTS"))
        .withColumn("TSTR",F.date_format($"StandartTS",formatTR))
        .withColumn("TSENG",F.date_format($"StandartTS",formatENG))
        .withColumn("BirYil",F.date_add($"StandartTS",365 ))
        .withColumn("Year",F.year($"StandartTS"))
        .withColumn("fark",F.datediff($"BirYil",$"StandartTS"))

    df2.show(10)
  }
}
