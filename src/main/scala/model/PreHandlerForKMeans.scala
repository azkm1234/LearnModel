package model

import java.io.File

import conf.Const
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.RecivedData

object PreHandlerForKMeans {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("PreHandlerForKMeans")
      .getOrCreate()
    val data = createData(spark)
    data.printSchema()
    data.show(5)
    val pipelineModel = getPipeline(spark, data)
    pipelineModel.transform(data).show(6)
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

  private def getPipeline(spark:SparkSession, data:DataFrame):PipelineModel = {
    val file = new File(Const.PREHANDLER_MODEL_PATH)
    if (file.exists()) {
      PipelineModel.load(file.getAbsolutePath)
    } else {
      val pipelineModel = createPipeline(spark).fit(data)
      pipelineModel.save(Const.PREHANDLER_MODEL_PATH)
      pipelineModel
    }
  }
  private def createPipeline(spark: SparkSession): Pipeline = {
    val indexer1 = new StringIndexer()
      .setInputCol("protocol_type")
      .setOutputCol("protocol_type_index")
    val encoder1 = new OneHotEncoder()
      .setInputCol("protocol_type_index")
      .setOutputCol("protocol_type_Vec")
    val indexer2 = new StringIndexer()
      .setInputCol("service")
      .setOutputCol("service_index")
    val encoder2 = new OneHotEncoder()
      .setInputCol("service_index")
      .setOutputCol("service_Vec")
    val indexer3 = new StringIndexer()
      .setInputCol("flag")
      .setOutputCol("flag_index")
    val encoder3 = new OneHotEncoder()
      .setInputCol("flag_index")
      .setOutputCol("flag_Vec")
    val indexer4 = new StringIndexer()
      .setInputCol("land")
      .setOutputCol("land_index")
    val encoder4 = new OneHotEncoder()
      .setInputCol("land_index")
      .setOutputCol("land_Vec")

    val featuresAssembler = new VectorAssembler()
      .setInputCols(Array("duration", "protocol_type_Vec", "service_Vec", "flag_Vec", "src_bytes", "dst_bytes", "land_Vec", "wrong_fragment", "urgent", "count", "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count", "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate", "dst_host_rerror_rate", "dst_host_srv_rerror_rate"))
      .setOutputCol(Const.features)

    val scaler = new StandardScaler()
      .setInputCol(Const.features)
      .setOutputCol(Const.scaledFeatures)
    new Pipeline().setStages(Array(indexer1, encoder1, indexer2,
      encoder2, indexer3, encoder3, indexer4, encoder4,
      featuresAssembler, scaler))
  }
}
