package highLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object schemaOps {
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
      .load("/home/basayev/Downloads/sparkDatas/OnlineRetail.csv")
        .withColumn("UnitPrice",regexp_replace($"UnitPrice",",","."))

    //df.show()
    //df.printSchema()

    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("sep",";")
      .option("header","true")
      .csv("/home/basayev/Downloads/sparkDatas/OnlineRetail2")


    val retailManualSchema = new StructType(
      Array(
        new StructField("InvoiceNo",StringType,true),
        new StructField("StockCode",StringType,true),
        new StructField("Description",StringType,true),
        new StructField("Quantity",IntegerType,true),
        new StructField("InvoiceDate",StringType,true),
        new StructField("UnitPrice",FloatType,true),
        new StructField("CustomerID",IntegerType,true),
        new StructField("Country",StringType,true)
      )
    )

    val dfFromfile = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      .schema(retailManualSchema)
      .load("/home/basayev/Downloads/sparkDatas/OnlineRetail2")

    dfFromfile.show()
    dfFromfile.printSchema()

  }
}
