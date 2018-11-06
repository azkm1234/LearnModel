package streaming

import conf.{Config, Const}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import util.RecivedData

object StreamingRandomForest {
  val scaledFeatures = "scaledFeatures"
  val features = "features"
  val label = "label"
  val kmeansPrediction = "kmeansPrediction"
  val distanceToCentersVector = "distanceToCentersVector"
  val assembledFeatures = "assemblerFeatures"
  val indexedLabel = "indexedLabel"
  val rfPrediction = "rfPrediction"
  val predictedLabel = "predictedLabel"

  val logger = LoggerFactory.getLogger(StreamingRandomForest.getClass)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaStreamingKmeans")
      .set(Const.maxRatePerPartitionKey, Const.maxRatePerPartition.toString)
      .setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(Const.BATCH_TIME))

//    val data: DStream[String] = getKafkaInputStream(Const.SOURCE_TOPIC,1,2,ssc)
    val data = createDstream(ssc)
    val pipeline1Model: PipelineModel = PipelineModel.load(Const.PIPELINE_1_MODEL_PATH)
    val pipeline2Model: PipelineModel  = PipelineModel.load(Const.PIPELINE_2_MODEL_PATH)
//    data.print(10)
    data.foreachRDD(rdd => {
      val spark = SparkSessionSingleton.getInstance(conf)
      rddDataHandler(rdd, spark, pipeline1Model,pipeline2Model)
    })
    ssc.start()
    ssc.awaitTermination()
  }
  def rddDataHandler(rdd:RDD[String], spark:SparkSession, pipeline1Model: PipelineModel, pipeline2Model: PipelineModel): Unit = {
    import spark.implicits._
    val data = rdd.flatMap{line => line.split("\n")}.map{line =>
      val strs = line.split(",")
      try {
        RecivedData(strs(0), strs(1), strs(2), strs(3).toDouble, strs(4), strs(5), strs(6), strs(7).toDouble, strs(8).toDouble, strs(9), strs(10).toDouble, strs(11).toDouble, strs(12).toDouble, strs(13).toDouble, strs(14).toDouble, strs(15).toDouble, strs(16).toDouble, strs(17).toDouble, strs(18).toDouble, strs(19).toDouble, strs(20).toDouble, strs(21).toDouble, strs(22).toDouble, strs(23).toDouble, strs(24).toDouble, strs(25).toDouble, strs(26).toDouble, strs(27).toDouble, strs(28).toDouble, strs(29).toDouble, strs(30).toDouble)
      } catch {
        case e:Exception => {
          logger.error("parse error line = {}",line)
          null
        }
      }
    }.filter(_ != null).toDF
    handleData(data, spark, pipeline1Model,  pipeline2Model)
  }

  def createDstream(ssc:StreamingContext): DStream[String] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Const.kafkaParams, Set(Const.SOURCE_TOPIC))
        .map(_._2)
  }

  def handleData(data:DataFrame, spark:SparkSession, pipeline1Model: PipelineModel, pipeline2Model: PipelineModel): Unit = {
    if (data.count() > 0) {
      val pipeline1Result: DataFrame = pipeline1Model.transform(data)
        .select("timeStamp", "srcIp", "desIp", "scaledFeatures", "kmeansPrediction") //toDo 这里需要改,增加"timeStamp", "srcIp", "desIp"
      pipeline1Result.printSchema()
      val kMeansModel = pipeline1Model.stages(11).asInstanceOf[KMeansModel]
      val testData = convertData(spark, pipeline1Result, kMeansModel, scaledFeatures)
      testData.printSchema()

      val labels = pipeline1Model.stages(9).asInstanceOf[StringIndexerModel].labels

      val pipe2Result = pipeline2Model.transform(testData).select("timeStamp", "srcIp", "desIp", predictedLabel)
      logger.error("pipe2Result")
      pipe2Result.printSchema()
//      val url = "jdbc:mysql://master:3306/hadoopdb"
//      val talbeName = "testTable"
//      val prop = new Properties()
//      prop.setProperty("user", "root")
//      prop.setProperty("password", "zjc1994")
//      pipe2Result.write.jdbc(url, talbeName, prop)
    }
  }


  /**
    * 计算每一调数据到每一个center的距离的平方，
    * 并且作为特征值与原来的ScaledFeatures相结合
    */
  private def convertData(spark: SparkSession, data: DataFrame, model: KMeansModel, colName: String): DataFrame = {
    val clusterCenters = model.clusterCenters
    val appendClusterCenter = udf((features: Vector) => {
      val r = clusterCenters.toArray.map { v1 =>
        Vectors.sqdist(v1, features)
      }
      Vectors.dense(r)
    })
    data.withColumn(distanceToCentersVector, appendClusterCenter(col(colName)))
  }

  def getKafkaInputStream(topic: String, numRecivers: Int,
                          partition: Int, ssc: StreamingContext): DStream[String] = {

    val topics = Map(topic -> partition / numRecivers)

    val kafkaDstreams: Seq[DStream[String]] = (1 to numRecivers).map { _ =>
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,
        Const.kafkaParams,
        topics,
        StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    }
    ssc.union(kafkaDstreams)
  }
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
