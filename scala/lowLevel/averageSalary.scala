package lowLevel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object averageSalary {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("conff")
      .setExecutorEnv("spark.executor.memory","4g")
      .setExecutorEnv("spark.driver.memory","2g")

    val sc = new SparkContext(conf)

    val simpleRdd = sc.textFile("/home/basayev/Downloads/simple_data.csv")
        .filter(!_.contains("sirano"))

    //simpleRdd.take(5).foreach(println)

    def meslekMaasPair(line:String)={
      val meslek = line.split(",")(3)
      val maas= line.split(",")(5).toDouble
      (meslek,maas)
    }

    val meslekMaasPairRdd = simpleRdd.map(meslekMaasPair)
    //meslekMaasPairRdd.take(5).foreach(println)

    val meslekMaas = meslekMaasPairRdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    //meslekMaas.take(10).foreach(println)

    val ortMaas = meslekMaas.mapValues(x => x._1 / x._2)

    ortMaas.take(10).foreach(println)

  }
}
