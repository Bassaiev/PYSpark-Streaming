package highLevel

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object sqlCsv {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("sqlCsv")
      .config("spark.driver.memory","4g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val dfFromfile = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      .option("inferSchema","true")
      .load("/home/basayev/Downloads/OnlineRetail.csv")

    dfFromfile.cache()

    dfFromfile.createOrReplaceTempView("tablo")

    spark.sql{
      """
        SELECT Country, SUM(Quantity) AS Quantity
        FROM tablo
        GROUP BY Country
        ORDER BY Quantity DESC
      """
    }.show(20)

    spark.sql{
      """
        SELECT Country, SUM(UnitPrice) AS UnitPrice
        FROM tablo
        GROUP BY Country
        ORDER BY UnitPrice DESC
      """
    }.show(20)
  }
}
