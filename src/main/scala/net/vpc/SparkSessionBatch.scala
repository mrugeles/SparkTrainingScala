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

    val struct =
      StructType(
        StructField("a", IntegerType, true) ::
          StructField("b", LongType, false) ::
          StructField("c", BooleanType, false) :: Nil)

    val data = spark.read.schema(struct).load("s3://")

    val events= spark.read.csv(getClass.getResource("/logs.txt").getPath)
      .withColumnRenamed("_c0", "ip")
      .withColumnRenamed("_c1", "timestamp")
      .withColumnRenamed("_c2", "guid")
      .withColumnRenamed("_c3", "url")
    import spark.implicits._
    val mapped = events.groupBy($"guid").count().repartition($"guid")
    mapped.sort(desc("count")).show(10)
    //mapped.show()
  }


}


object SparkSessionBatch{
  def main(args: Array[String]): Unit = {
    new SparkSessionBatch().start
  }
}
