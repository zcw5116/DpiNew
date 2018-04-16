package com.zyuc.dpi.query

import java.text.SimpleDateFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by zhoucw on 上午11:19.
  */
object AccessTextLogQuery {

  val logger = Logger.getLogger("accessQuery")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("test").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val hid = sc.getConf.get("spark.app.houseid", "1000")
    val beginTime = sc.getConf.get("spark.app.beginTime", "2017-11-04 20:01:16")
    val endTime = sc.getConf.get("spark.app.endTime", "2017-11-04 20:55:50")
    val inputPath = sc.getConf.get("spark.app.inputFiles", "/tmp/input/access-src/0800/1000,/tmp/input/access-src/0805/1000")
    val batchid = sc.getConf.get("spark.app.batchid", System.currentTimeMillis().toString)
    logger.info("batchid:" + batchid)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val beginUnix = sdf.parse(beginTime).getTime/1000
    val endUnix = sdf.parse(beginTime).getTime/1000

    val df = getQueryDF(spark, inputPath, beginUnix.toString, endUnix.toString)
    df.show()
    df.write.format("csv").mode(SaveMode.Overwrite).options(Map("sep"->",")).save("/tmp/zhou/" + "1112/" + "2223")
   // df.write.format("csv").mode(SaveMode.Overwrite).options(Map("sep"->","))save("/tmp/zhou/" + batchid)

  }

  /**
    * 根据根据时间返回DataFrame
    *
    * @param spark
    * @param beginTime
    * @param endTime
    * @return
    */
  def getQueryDF(spark: SparkSession, inputPath: String, beginTime: String, endTime: String): DataFrame = {
    val rdd = spark.sparkContext.textFile(inputPath).map(x=>x.split("\\|")).
      filter(_.length == 10).map(x=>(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9)))
    import spark.implicits._
    val df = rdd.toDF("hid", "srcip", "destip","proctype","srcport", "destport","domain","url","duration","acctime")
    df.filter(s"acctime>='$beginTime' and acctime<='$endTime' ")
  }




}
