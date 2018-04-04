package com.zyuc.dpi.query

import java.text.SimpleDateFormat
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable

/**
  * Created by zhoucw on 上午11:19.
  */
object AccessLogQeury {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("test").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    //val hid = sc.getConf.get("spark.app.houseid", "1000")
    val beginTime = "2017-11-04 20:01:16" //sc.getConf.get("spark.app.begin", "20180326120550")
    val endTime = "2017-11-04 20:55:50"
    // sc.getConf.get("spark.app.end", "20180326121455")
    val hid = 1000
    val inputPath = "/tmp/output/accesslog1/data/hid=10018"
    //val accessTable = sc.getConf.get("spark.app.accessTable", "dpi_log_access_m5")

    val df = getQueryDF(spark, inputPath, beginTime, endTime)

    df.show(false)
    println(df.count())
  }


  /**
    * 根据unixtime返回时间分区
    *
    * @param unixtime
    * @return
    */
  def getM5Partition(unixtime: Long): String = {
    val targetDf = new SimpleDateFormat("yyyyMMddHHmm")
    val yyyyMMddHHmm = targetDf.format(unixtime)
    val d = yyyyMMddHHmm.substring(2, 8)
    val h = yyyyMMddHHmm.substring(8, 10)
    val m5 = yyyyMMddHHmm.substring(10, 11) + (yyyyMMddHHmm.substring(11, 12).toInt / 5) * 5
    "/d=" + d + "/h=" + h + "/m5=" + m5
  }

  /**
    * 根据查询条件获取要扫描的分区
    *
    * @param begin 精确到秒
    * @param end   精确到秒
    * @return
    */
  def getPartitions(begin: Long, end: Long): mutable.HashSet[String] = {
    val partitionSet = new mutable.HashSet[String]()

    var time: Long = begin
    while (time < end) {
      partitionSet.+=(getM5Partition(time))
      time = time + 5 * 60 * 1000
    }
    partitionSet.+=(getM5Partition(end))
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
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val begin = sdf.parse(beginTime).getTime
    val end = sdf.parse(endTime).getTime
    val beginPartition = getM5Partition(begin)
    val endPartition = getM5Partition(end)
    var resultDF: DataFrame = null

    // ########################################################################
    //   开始和结束的分区， 加上时间过滤， 提升效率
    // ########################################################################
    val beginDF = loadFilesToDF(spark, inputPath + beginPartition)
    val endDF = loadFilesToDF(spark, inputPath + endPartition)
    if(beginDF != null && endDF != null){
      resultDF = beginDF.filter(s"acctime>='$beginTime'").union(endDF.filter(s"acctime<='$endTime'"))
    }else if(beginDF != null){
      resultDF = beginDF.filter(s"acctime>='$beginTime'")
    }else if(endDF != null){
      resultDF = endDF.filter(s"acctime<='$endTime'")
    }

    // ########################################################################
    //   开始和结束的分区以外分区
    // ########################################################################
    val partitionSet = getPartitions(begin, end)
    partitionSet.foreach(p => {
      if(p!=beginPartition && p!=endPartition){
        val df = loadFilesToDF(spark, inputPath + p)
        if(resultDF==null){
          resultDF = df
        }else if(df != null){
          resultDF = resultDF.union(df)
        }
      }
    })
    resultDF
  }


  def loadFilesToDF(spark: SparkSession, inputPath: String): DataFrame = {
     var df:DataFrame = null
    try{
      df = spark.read.format("orc").load(inputPath)
    } catch {
      case e:Exception => {
        e.printStackTrace()
      }
    }
    df.coalesce(1)
  }


  def loadDF(spark: SparkSession, resultDF: DataFrame, inputPath: String): DataFrame = {
    var df: DataFrame = null
    println(inputPath)
    try {
      var partDF = spark.read.format("orc").load(inputPath)
      if (resultDF != null) {
        df = resultDF.union(partDF)
      } else {
        df = partDF
      }
    } catch {
      case e: Exception => {
        df = resultDF
      }
    }
   df
  }
}
