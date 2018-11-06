package util
import java.util.{Properties, Random}

import conf.ConfUtils
import kafka.producer.ProducerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}

import scala.collection.mutable;
class ProducerPool(val capacity:Integer, props:Properties) {
  val freeList = new mutable.ListBuffer[Producer[Integer, String]]
  val busyList = new mutable.ListBuffer[Producer[Integer, String]]
  private def init() = {
    this.synchronized{
      for (i <- Range(0, capacity)) {
        freeList += new KafkaProducer[Integer, String](ProducerPool.props)
      }
    }
  }
  private def getProducerHelper():Producer[Integer, String] = {
    this.synchronized{
      if (this.freeList.isEmpty) {
        null
      } else {
        val producer = this.freeList.remove(0)
        busyList += producer
        producer
      }
    }
  }
  private def getProducer():Producer[Integer, String] = {
    var producer:Producer[Integer, String] = null
    while (producer == null) {
      producer = getProducerHelper()
      Thread.sleep(ProducerPool.random.nextInt(10) + 20)
    }
    producer
  }
  def releaseProducer(producer:Producer[Integer, String]):Unit = {
    if (producer == null) {
      return
    }
    synchronized{
      this.freeList += producer
      this.busyList -= producer
    }
  }
}

object ProducerPool {
  val props = getProps()
  val random = new Random(System.currentTimeMillis())
  val producerPool = ProducerPool(10, ProducerPool.props)
  private def getProps():Properties = {
    val props = ConfUtils.loadpropertiesFile("kafka-producer.properties")
    props
  }
  def getProducer():Producer[Integer, String] = {
    producerPool.getProducer()
  }
  def relase(producer:Producer[Integer, String]) = {
    producerPool.releaseProducer(producer)
  }

  def apply(capacity: Integer, props: Properties): ProducerPool = {
    var producerPool = new ProducerPool(capacity, props)
    producerPool.init()
    producerPool
  }
}
