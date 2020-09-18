package highLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object dataframeGi {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder()
      .appName("Dataframe")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val dfFromList = sc.parallelize(List(1,2,3,4,5,6,7,3,4,5)).toDF("rakamlar")
    //dfFromList.printSchema()

    val dfFromSpark = spark.range(10,100,5).toDF("penta")
    //dfFromSpark.printSchema()

    val dfFromFile = spark.read.format("csv")
      .option("sep",";")
      .option("header","true")
      .option("inferSchema","true")
      .load("/home/basayev/Downloads/OnlineRetail.csv")


    //dfFromFile.printSchema()
    //dfFromFile.show(10,false)
    //println("\nLine Count:"+dfFromFile.count())

    //dfFromFile.select("InvoiceNo","Quantity").show(10)

    //dfFromFile.select("Quantity").sort($"Quantity").show(10)

    spark.conf.set("spark.sql.shuffle.partitions","5")

    dfFromFile.sort("Quantity").explain()

  }
}
