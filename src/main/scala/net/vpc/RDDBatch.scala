package net.vpc

import org.apache.spark.{SparkConf, SparkContext}

class RDDBatch {


  def start = {
    val config = new SparkConf()
      .setAppName("rddBatch")
      .setMaster("local[*]")
    val sc = new SparkContext(config)

    val logs = sc.textFile(getClass.getResource("/logs.txt").getPath)
    val map = logs.map(line => {
      val data = line.split(",")
      (data(2), 1)
    })

    val reduce = map.reduceByKey((a, b) => a+b).sortBy(t => t._2, false)
    reduce.take(10).foreach(t => println(t))
    sc.stop()
  }
}

object RDDBatch{
  def main(args: Array[String]): Unit = {
    new RDDBatch().start
  }
}
