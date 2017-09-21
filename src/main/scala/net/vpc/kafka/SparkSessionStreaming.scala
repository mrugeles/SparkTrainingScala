package net.vpc.kafka

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.desc

/**
  * Created by vpctraining on 21/09/17.
  */

case class Log(ip:String, timestamp: String, id:String, path:String)
class SparkSessionStreaming {
  def start = {
    val spark = SparkSession
      .builder()
      .appName("SparkSessionBatch")
      .master("local[*]")
      .getOrCreate()

    val events = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "logs")
      .load()


    //val grouped = events.groupBy("path").count().orderBy(desc("count"))
    //grouped.show(10)
    val values = events.selectExpr("CAST(value AS STRING)")
    import spark.implicits._
    val records = values.map(line => {
     val data = line.getString(0).split(",")
     Log(data(0), data(1), data(2), data(3))
    })

    val grouped = records.groupBy("path").count().orderBy(desc("count"))
    val query = grouped.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}

object SparkSessionStreaming{
  def main(args: Array[String]): Unit = {
    new SparkSessionStreaming().start
  }
}
