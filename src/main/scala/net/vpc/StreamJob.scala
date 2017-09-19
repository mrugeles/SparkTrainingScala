package net.vpc

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by mrugeles on 30/06/2017.
  */
class StreamJob {

  def start = {
    val sparkConf = new SparkConf()
      .setAppName("StreamJob")
      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
  }
}

object StreamJob{
  def main(args: Array[String]): Unit = {
    new StreamJob().start
  }
}
