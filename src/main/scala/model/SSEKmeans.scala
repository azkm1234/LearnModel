package model

import conf.Const
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.RecivedData

//计算SSE，手肘法和轮廓系数法 决定最佳的K值
object SSEKmeans {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("SSEKmeans")
      .getOrCreate()
    val df = createData(spark)
    val prehandler = PipelineModel.load(Const.PREHANDLER_MODEL_PATH)
    val data = prehandler.transform(df).select("scaledFeatures")
    val kmeans = new KMeans()
      .setK(2)
      .setSeed(1L)
      .setFeaturesCol("scaledFeatures")
      .setPredictionCol("predict")
      .setInitMode("k-means||")
    val model = kmeans.fit(data)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(data)
    println(s"Within Set Sum of Squared Errors = $WSSSE")
    data.show(5)

    spark.stop()
  }
  private def createData(spark:SparkSession): DataFrame = {
    val text = spark.sparkContext
      .textFile(Const.PREHANDLER_TRAIN_DATA_PATH)
    val count = text.count()
    import spark.implicits._
    val data = text.map(_.split(","))
      .map(x => new RecivedData(
        x(0), x(1), x(2), x(3).toDouble, x(4),
        x(5), x(6), x(7).toDouble, x(8).toDouble, x(9),
        x(10).toDouble, x(11).toDouble, x(12).toDouble, x(13).toDouble, x(14).toDouble,
        x(15).toDouble, x(16).toDouble, x(17).toDouble, x(18).toDouble, x(19).toDouble,
        x(20).toDouble, x(21).toDouble, x(22).toDouble, x(23).toDouble, x(24).toDouble,
        x(25).toDouble, x(26).toDouble, x(27).toDouble, x(28).toDouble, x(29).toDouble,
        x(30).toDouble
      )).toDF
    data
  }
}
