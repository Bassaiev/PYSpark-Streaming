package MachineLearning.Classification

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.{SparkSession}

object IrisClassification {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("IrisClassification")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    val df = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("/home/basayev/Downloads/sparkDatas/iris.csv")

    println("Original DF: ")
    df.show(5)
    df.describe().show()
    df.groupBy("Species").count().show()
    df.printSchema()


    import org.apache.spark.ml.feature.StringIndexer

    val stringIndexer = new StringIndexer()
      .setInputCol("Species")
      .setOutputCol("label")
      .setHandleInvalid("skip")

    val labelDF = stringIndexer.fit(df).transform(df)
    println("labelDF:")
    labelDF.show(5)

    import org.apache.spark.ml.feature.VectorAssembler

    val assembler = new VectorAssembler()
      .setInputCols(Array("SepalLengthCm","SepalWidthCm","PetalLengthCm","PetalWidthCm"))
      .setOutputCol("features")

    val  vectorDF = assembler.transform(labelDF)
    println("vectorDF:")
    vectorDF.show(5)

    val Array(trainDF, testDF) = vectorDF.randomSplit(Array(0.8, 0.2),142L)

    import org.apache.spark.ml.classification.LogisticRegression

    val logRegObj = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")

    val logRegModel = logRegObj.fit(trainDF)

    val transformedDF = logRegModel.transform(testDF)

    println("transformedDF")
    transformedDF.show(false)

    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

    val evaluator  = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val accuracy = evaluator.evaluate(transformedDF)

    println("Accuracy", accuracy)


  }
}
