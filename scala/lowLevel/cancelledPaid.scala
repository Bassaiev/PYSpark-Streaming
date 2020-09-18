package lowLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object cancelledPaid {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("conff")
      .setExecutorEnv("spark.executor.memory","4g")
      .setExecutorEnv("spark.driver.memory","2g")

    val sc = new SparkContext(conf)

    val retailRDD=sc.textFile("/home/basayev/Downloads/OnlineRetail.csv")
      .filter(!_.contains("InvoiceNo"))

    val retailTotal = retailRDD.map(x => {
      var isCancelled:Boolean = if(x.split(";")(0).startsWith("C")) true else false
      var quantity:Double = x.split(";")(3).toDouble
      var price:Double = x.split(";")(5).replace(",",".").toDouble

      var total:Double = quantity*price
      (isCancelled, total)
    })

    retailTotal.take(5).foreach(println)

    println("\nİptal edilen toplam tutar: ")
    retailTotal.map(x => (x._1, x._2))
      .reduceByKey((x,y) => x + y)
      .filter(x => x._1 == true)

      .map(x => x._2)
      .take(5)
      .foreach(println)
  }
}
