package lowLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object filterWords {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("conff")
      .setExecutorEnv("spark.executor.memory","4g")
      .setExecutorEnv("spark.driver.memory","2g")

    val sc = new SparkContext(conf)

    val retailRddwithHeader=sc.textFile("/home/basayev/Downloads/OnlineRetail.csv")

    val retailRdd = retailRddwithHeader.mapPartitionsWithIndex(
      (idx, iter) => if (idx==0) iter.drop(1) else iter
    )

    //retailRdd.take(5).foreach(println)

    //retailRdd.filter(x => x.split(";")(3).toInt > 30).take(5).foreach(println)

    retailRdd.filter(x => x.split(";")(2).contains("COFFEE") &&
      x.split(";")(5).trim.replace(",",".")
        .toFloat > 20.0F).take(5).foreach(println)


    def coffeePrice20(line:String):Boolean={
      var sonuc = true
      var Description:String = line.split(";")(2)
      var unitPrice:Float = line.split(";")(5).trim.replace(",",".").toFloat

      sonuc = Description.contains("COFFEE") && unitPrice > 20.0
      sonuc
    }

    retailRdd.filter(x => coffeePrice20(x))
      .take(5)
      .foreach(println)


  }
}
