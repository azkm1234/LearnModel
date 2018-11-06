package streaming

import conf.Const
import conf.Const.kafkaParams
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.LoggerFactory
import streaming.KMeansModelPre.createDstream
import util.ProducerPool

object KMeansModel {
  val logger = LoggerFactory.getLogger(KMeansModelPre.getClass)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KMeansModel").setMaster("local[3]")
      .set(Const.maxRatePerPartitionKey, Const.maxRatePerPartition.toString)
    val ssc = new StreamingContext(conf, Seconds(Const.BATCH_TIME))
    val inputStream: DStream[String] = createDstream(ssc)
    val dataStream = inputStream.map{line =>
      val array = line.split(" ")
      val vector = Vectors.parse(array(array.length - 1))
      (array.slice(0, array.length - 1).reduce((x1, x2) => x1 + " " + x2), vector)
    }
    val model: StreamingKMeans = new StreamingKMeans()
      .setK(30)
      .setDecayFactor(0.95)
      .setRandomCenters(64, 1)
    model.trainOn(dataStream.map(_._2))

    dataStream.foreachRDD((rdd, time) => {
      val producer = ProducerPool.getProducer()
      producer.send(new ProducerRecord[Integer, String](Const.WEIGHT_TOPIC, model.latestModel().clusterWeights.mkString("," )))
      println(model.latestModel().clusterWeights.mkString(","))
      ProducerPool.relase(producer)
    })

    model.predictOnValues(dataStream).map(l => l._1 + " " + l._2)
      .foreachRDD{rdd =>
        rdd.foreachPartition{it =>
          val producer = ProducerPool.getProducer()
          it.foreach(msg => producer.send(new ProducerRecord[Integer, String](Const.RESUT1_TOPIC, msg)))
          ProducerPool.relase(producer)
        }
      }

    ssc.start()
    ssc.awaitTermination()

  }
  def createDstream(ssc:StreamingContext): DStream[String] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(Const.MID_TOPIC))
      .map(_._2)
  }
}
