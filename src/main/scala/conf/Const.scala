package conf

object Const {
  val NUM_OF_CLUSTERS = Config.getInt("numOfClusters") //在random-foreast中使用
  val NUM_OF_CENTERS = Config.getInt("numOfCenters") //Streaming Kmeans中使用
  val DECAY_FACTOR = Config.getFloat("decayFactor")
  val DIMENS = 64

  val PIPELINE_2_MODEL_PATH = Config.getString("pipeline2ModelPath")
  val PIPELINE_1_MODEL_PATH = Config.getString("pipeline1ModelPath")
  val PREHANDLER_TRAIN_DATA_PATH = Config.getString("prehandler.train.data.path")
  val PREHANDLER_MODEL_PATH = Config.getString("prehandler.model.path")

  val NAMES_FILE = "kddcup.names.txt"
  val SOURCE_TOPIC = Config.getString("source_topic")
  val DESC_TOPIC = Config.getString("desc_topic")
  val BATCH_TIME:Int = Config.getInt("batchTime")
  val MID_TOPIC = Config.getString("mid_topic")
  val RESUT1_TOPIC = Config.getString("result1_topic")
  val RESULT2_TOPIC = Config.getString("result2_topic")
  val WEIGHT_TOPIC = Config.getString("weight_topic")
  val scaledFeatures = "scaledFeatures"
  val features = "features"
  val label = "label"
  val kmeansPrediction = "kmeansPrediction"
  val distanceToCentersVector = "distanceToCentersVector"
  val assembledFeatures = "assemblerFeatures"
  val indexedLabel = "indexedLabel"
  val rfPrediction = "rfPrediction"
  val predictedLabel = "predictedLabel"
  val maxRatePerPartitionKey = "spark.streaming.kafka.maxRatePerPartition"
  val maxRatePerPartition = Config.getInt(maxRatePerPartitionKey)
  val kafkaParams = Map(
    "zookeeper.connect" -> Config.getString("zookeeper.connect"),
    "group.id" -> Config.getString("group.id"),
    "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
    "zookeeper.session.timeout.ms" -> "500",
    "auto.commit.interval.ms" -> "1000",
    "zookeeper.sync.time.ms" -> "250",
    "auto.offset.reset" -> "smallest",
    //    "auto.offset.reset" -> "largest",
    "zookeeper.connection.timeout.ms" -> "30000",
    "fetch.message.max.bytes"-> (1024 * 1024 * 50).toString
  )

}
