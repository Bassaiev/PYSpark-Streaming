package lowLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object joinops {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setAppName("join")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    val orderItemsRdd = sc.textFile("/home/basayev/Downloads/order_items.csv")
      .filter(!_.contains("orderItemName"))

    val productRdd = sc.textFile("/home/basayev/Downloads/products.csv")
      .filter(!_.contains("productId"))

    def makeOrderItemsPairRdd(line:String)={
      val orderItemName = line.split(",")(0)
      val orderItemOrderId = line.split(",")(1)
      val orderItemProductId = line.split(",")(2)
      val orderItemQuantity = line.split(",")(3)
      val orderItemSubTotal = line.split(",")(4)
      val orderItemProductPrice = line.split(",")(5)
      (orderItemProductId,(orderItemName,orderItemOrderId,orderItemQuantity,orderItemSubTotal,orderItemProductPrice))
    }

    val orderItemsPairRdd = orderItemsRdd.map(makeOrderItemsPairRdd)
    //orderItemsPairRdd.take(5).foreach(println)

    def makeProductsPairRDD(line:String) ={
      val productId = line.split(",")(0)
      val productCategoryId = line.split(",")(1)
      val productName = line.split(",")(2)
      val productDescription = line.split(",")(3)
      val productPrice = line.split(",")(4)
      val productImage = line.split(",")(5)

      (productId, (productCategoryId, productName, productDescription, productPrice, productImage))
    }

    val productsPairRDD = productRdd.map(makeProductsPairRDD)

    val joinRdd =  orderItemsPairRdd.join(productsPairRDD)

    joinRdd.take(10).foreach(println)

  }
}
