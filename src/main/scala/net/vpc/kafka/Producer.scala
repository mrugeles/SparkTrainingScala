package net.vpc.kafka

import java.util

import net.vpc.Generator
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

class Producer {
  val kafkaTopic = "logs"
  val kafkaBrokers = "localhost:9092"

  val recordsPerSecond = 500
  val numSecondsToSend = 60

  val props = new util.HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")

  def start = {
    val generator = new Generator
    val producer = new KafkaProducer[String, String](props)
    while (true){
      val log = generator.getRandomLog
      val message = new ProducerRecord[String, String](kafkaTopic, null, s"${log.ip},${log.datetime},${log.guid},${log.url}")
      producer.send(message)

    }
  }
}

object KafkaProducer{
  def main(args: Array[String]): Unit = {
    new Producer().start
  }
}
