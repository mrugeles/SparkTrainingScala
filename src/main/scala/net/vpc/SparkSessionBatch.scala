package net.vpc
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
/**
  * Created by mrugeles on 30/06/2017.
  */
class SparkSessionBatch {

  def start = {
    val spark = SparkSession
      .builder()
      .appName("SparkSessionBatch")
      .master("local[*]")
      .getOrCreate()

    val events = spark.read.option("header", "true")
      .csv(getClass.getResource("/logs.txt").getPath)

    val grouped = events.groupBy("path").count().orderBy(desc("count"))

    grouped.show(10)
  }


}


object SparkSessionBatch{
  def main(args: Array[String]): Unit = {
    new SparkSessionBatch().start
  }
}
