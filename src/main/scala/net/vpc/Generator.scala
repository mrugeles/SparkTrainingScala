package net.vpc

import java.io.FileWriter
import java.util.Calendar

import scala.io.Source

/**
  * Created by mrugeles on 30/06/2017.
  */

class Generator(){
  case class Log(ip: String, datetime: Long, guid:String, url:String){}
  val urlList = Array("/init", "/play","/record","/end","/rewind","/stop","/pause","/download","/pay","/addUser","/forward","/logout","/login","/subscribe")
  lazy val users = Source.fromURL(getClass.getResource("/users.csv")).getLines().toArray

  def writeToFile(path: String, line:String) = {
    val classLoader = getClass.getClassLoader

    val fw = new FileWriter("C:\\Users\\mrugeles\\Documents\\Spark\\vpc-training\\SparkTraining\\src\\main\\resources\\logs.txt", true)
    try {
      fw.write( line)
    }
    finally fw.close()
  }


  def generate = {
    (1 to 1000000).foreach( n => {
      val log  = getRandomLog
      writeToFile("/logs.txt", s"${log.ip},${log.datetime},${log.guid},${log.url}\n")
      println("")
    })
  }

  def getRandomLog:Log = {
    val r = scala.util.Random
    val userGuid = users(r.nextInt(users.length)).split(",").head
    Log(getRandomIp, getRandomDatetime, userGuid, getRandomUrl)
  }

  def getRandomIp:String = {
    val r = scala.util.Random
    s"${r.nextInt(256)}.${r.nextInt(256)}.${r.nextInt(256)}.${r.nextInt(256)}"
  }
  def getRandomDatetime:Long = {
    Calendar.getInstance().getTimeInMillis()
  }

  def getRandomUrl: String = {
    val r = scala.util.Random
    urlList(r.nextInt(urlList.length))
  }

  def getRandomUser: String = {
    val r = scala.util.Random
    urlList(r.nextInt(urlList.length))
  }
}
object Generator {
  def main(args: Array[String]): Unit = {
    new Generator().generate
  }
}
