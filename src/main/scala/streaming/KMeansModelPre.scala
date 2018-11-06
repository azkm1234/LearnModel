package streaming

import com.sun.rowset.internal.Row
import conf.Const
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import conf.Const.kafkaParams
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import streaming.StreamingRandomForest.SparkSessionSingleton
import util.{ProducerPool, RecivedData}

object KMeansModelPre {
    def main(args:Array[String]) = {
      val logger = LoggerFactory.getLogger(KMeansModelPre.getClass)
      val conf = new SparkConf().setAppName("KMeansModelPre").setMaster("local[3]")
        .set(Const.maxRatePerPartitionKey, Const.maxRatePerPartition.toString)
      val ssc = new StreamingContext(conf, Seconds(Const.BATCH_TIME))
      val dataStream: DStream[String] = createDstream(ssc)

      val tmp: DStream[Array[String]] = dataStream
        .flatMap(_.split("\n"))
        .map(_.split(","))


      val model = PipelineModel.load(Const.PREHANDLER_MODEL_PATH)
      dataStream.foreachRDD{rdd =>
        val spark = SparkSessionSingleton.getInstance(conf)
        import spark.implicits._
        val dataframe = rdd.flatMap(_.split("\n")).map(_.split(","))
          .map(x => new RecivedData(
            x(0), x(1), x(2), x(3).toDouble, x(4),
            x(5), x(6), x(7).toDouble, x(8).toDouble, x(9),
            x(10).toDouble, x(11).toDouble, x(12).toDouble, x(13).toDouble, x(14).toDouble,
            x(15).toDouble, x(16).toDouble, x(17).toDouble, x(18).toDouble, x(19).toDouble,
            x(20).toDouble, x(21).toDouble, x(22).toDouble, x(23).toDouble, x(24).toDouble,
            x(25).toDouble, x(26).toDouble, x(27).toDouble, x(28).toDouble, x(29).toDouble,
            x(30).toDouble
          )).toDF
        val result = model.transform(dataframe).select("timeStamp", "srcIp", "desIp", "scaledFeatures")
        result.show(5)
        result.foreachPartition{x =>
          val producer = ProducerPool.getProducer()
          x.foreach{ row =>
            val msg = StringBuilder.newBuilder
                .append(row.getString(0)).append(" ")
                .append(row.getString(1)).append(" ")
                .append(row.getString(2)).append(" ")
                .append(row.getAs[org.apache.spark.ml.linalg.Vector](3).toString).toString()
              producer.send(new ProducerRecord[Integer, String](Const.MID_TOPIC, msg))
          }
          ProducerPool.relase(producer)
        }
      }

      ssc.start()
      ssc.awaitTermination()
    }

  def createDstream(ssc:StreamingContext): DStream[String] = {
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(Const.SOURCE_TOPIC))
      .map(_._2)
  }

}
