package net.vpc

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by mrugeles on 30/06/2017.
  */
class StreamJob {

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092,",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "my_app",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def start = {

    val topics = Array("logs")

    val sparkConf = new SparkConf()
      .setAppName("StreamJob")
      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val lofInfo = stream.map(record => {
      val log = record.value().split(",")
      (log(1), log(3))
    })

    lofInfo.foreachRDD(rdd => {
      rdd.foreach(info => {
        println(info)
      })

    })

    ssc.start()
    ssc.awaitTermination()
  }
}

object StreamJob{
  def main(args: Array[String]): Unit = {
    new StreamJob().start
  }
}
