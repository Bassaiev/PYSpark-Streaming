package lowLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object rddWordcount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("wordcount")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    val sc = spark.sparkContext

    val hikayeRDD  = sc.textFile("/home/basayev/Downloads/hikaye.txt")
    println(hikayeRDD.count())

    val kelimeler = hikayeRDD.flatMap(satir => satir.split(" "))

    val kelimeSayi = kelimeler.map(kelime => (kelime,1)).reduceByKey((x,y) => x+y)
    println(kelimeSayi.count())

    //kelimeSayi.take(10).foreach(println)

    val kelimeSayi2 = kelimeSayi.map(x => (x._2, x._1))
    //kelimeSayi2.take(10).foreach(println)

    kelimeSayi2.sortByKey(false).take(10).foreach(println)
  }
}
