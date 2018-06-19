package com.zyuc.dpi.query

import java.text.SimpleDateFormat
import java.util.{Base64, Date}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

/**
  * Created by zhoucw on 上午11:19.
  */
object AccessOrcLogQuery {

  val logger = Logger.getLogger("accessQuery")

  def main(args: Array[String]): Unit = {

    var begin = new Date().getTime
    println("#########################################begin:" + begin)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
   // val spark = SparkSession.builder().master("local[2]").appName("test").enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext

    val hid = sc.getConf.get("spark.app.hid", "10018")
    val beginTime = sc.getConf.get("spark.app.beginTime", "20171104150116")
    val endTime = sc.getConf.get("spark.app.endTime", "20171104205550")
    val basePath = sc.getConf.get("spark.app.basePath", "/tmp/output/accesslog1/data")
    val outputOrcPath = sc.getConf.get("spark.app.outputOrcPath", "/tmp/zhou")
    val ifCalCnt = sc.getConf.get("spark.app.ifCalCnt", "1")
    val batchid = sc.getConf.get("spark.app.batchid", "1234")
    val url = sc.getConf.get("spark.app.url", "NI") // http://kk.90wd.cn:883/data/kj83_com.js?_=1509798840672
    val domain = sc.getConf.get("spark.app.domain", "222.186.134.226:8011")
    val srcIpBegin = sc.getConf.get("spark.app.srcIpBegin", "114.101.178.156")
    val srcIpEnd = sc.getConf.get("spark.app.srcIpEnd", "183.166.237.216")
    val destIpBegin =sc.getConf.get("spark.app.destIpBegin", "222.186.134.226")
    val destIpEnd = sc.getConf.get("spark.app.destIpEnd", "222.186.134.228")
    val destPort = sc.getConf.get("spark.app.destPort", "8011")
    val partitionNum = sc.getConf.get("spark.app.partitionNum", "-1")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)


    val pathArr = getLoadPath(basePath + "/hid=" + hid, fileSystem, beginTime, endTime)
    println("pathArr:" + pathArr.length)
    pathArr.foreach(println)


    //val orcSrcDF = spark.read.format("orc").load(pathArr:_*)
    val orcSrcDF = spark.read.format("orc").options(Map("basePath"->basePath)).load(pathArr:_*)
    //var orcSrcDF = AccessOrcLogQuery.getQueryDF(spark, inputOrcPath + "/hid=" + hid, targetBegin, targetEnd)

    val savePath = outputOrcPath + "/" + batchid

    var condition = "1=1 "
    if (url != "NI") {
      val b64Url = Base64.getEncoder.encodeToString(url.getBytes())
     // condition =  condition + s" and url like '${b64Url}%'"
      condition =  condition + s" and url = '${b64Url}'"
    }

    if (domain != "NI") {
      condition = condition + s" and domain='${domain}'"
    }

    if (srcIpBegin != "NI") {
      val srcIpBegin2Long = ip2Long(srcIpBegin)
      condition = condition + s" and udf_ip2long(srcip)>=${srcIpBegin2Long}"
    }

    if (srcIpEnd != "NI") {
      val srcIpEnd2Long = ip2Long(srcIpEnd)
      condition = condition + s" and udf_ip2long(srcip)<=${srcIpEnd2Long}"
    }

    if (destIpBegin != "NI") {
      val destIpBegin2Long = ip2Long(destIpBegin)
      condition = condition + s" and udf_ip2long(destip)>=${destIpBegin2Long}"
    }

    if (destIpEnd != "NI") {
      val destIpEnd2Long = ip2Long(destIpEnd)
      condition = condition + s" and udf_ip2long(destip)<=${destIpEnd2Long}"
    }

    if (destPort != "NI") {
      condition = condition + s" and destport='${destPort}'"
    }

    def ip2Long(strIp: String) = {
      var result: Long = 0
      try {
        if (strIp != null && !"".equals(strIp.trim())) {
          val position1 = strIp.indexOf(".");
          val position2 = strIp.indexOf(".", position1 + 1);
          val position3 = strIp.indexOf(".", position2 + 1);

          if (position1 > -1 && position2 > -1 && position3 > -1) {
            val ip0 = strIp.substring(0, position1).toLong
            val ip1 = strIp.substring(position1 + 1, position2).toLong
            val ip2 = strIp.substring(position2 + 1, position3).toLong
            val ip3 = strIp.substring(position3 + 1).toLong
            result = (ip0 << 24) + (ip1 << 16) + (ip2 << 8) + ip3
          }
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
      result
    }

    spark.udf.register("udf_ip2long", (strIp: String) => {
      ip2Long(strIp)
    })


    val resultDF = orcSrcDF.filter(condition)
    if(partitionNum=="-1"){
      resultDF.write.format("csv").mode(SaveMode.Overwrite).options(Map("sep" -> ",")).save(savePath)
    }else{
      resultDF.repartition(partitionNum.toInt).write.format("csv").mode(SaveMode.Overwrite).options(Map("sep" -> ",")).save(savePath)

    }

    val queryTime = new Date().getTime - begin

    begin = new Date().getTime
    fileSystem.globStatus(new Path(savePath + "/*")).foreach(p=>{
      if(p.getLen==0){
        fileSystem.delete(p.getPath, false)
      }
    })
    val delTime = new Date().getTime - begin


    begin = new Date().getTime

    var cnt = 0l
    if (ifCalCnt == "1") {
      cnt = spark.read.format("csv").options(Map("sep" -> ",")).load(savePath).count()
    }
    val calcTime = new Date().getTime - begin


    fileSystem.rename(new Path(savePath), new Path(savePath + "_" + cnt + "_" + queryTime + "_" + calcTime + "_" + delTime))
   //resultDF.show()
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
    * 根据开始时间和结束时间生成hdfs目录， 并将目录保存到数组中
    * 如果分区目录不存在, 剔除
    * @param inputPath
    * @param fileSystem
    * @param beginTime  yyyyMMddHHmmss
    * @param endTime    yyyyMMddHHmmss
    * @return
    */
  def getLoadPath(inputPath: String, fileSystem:FileSystem, beginTime: String, endTime: String): Array[String] = {

    val sdf = new SimpleDateFormat("yyyyMMddHHmmSS")
    val begin = sdf.parse(beginTime).getTime
    val end = sdf.parse(endTime).getTime

    // 使用set保存分区目录
    val partitionSet = new mutable.HashSet[String]()

    /**
      *  根据时间生成分区目录
      *  example:
      *          参数： 201806130821的unixtime, 返回结果根据5分钟向下取整数
      *          返回分区目录： /hadoop/accesslog_etl/output/data/hid=1005/d=180613/h=08/m5=20
      * @param time
      * @return
      */
    def getPathByTime(time:Long):String = {
      val yyyyMMddHHmm = sdf.format(time)
      val d = yyyyMMddHHmm.substring(2, 8)
      val h = yyyyMMddHHmm.substring(8, 10)
      val m5 = yyyyMMddHHmm.substring(10, 11) + (yyyyMMddHHmm.substring(11, 12).toInt / 5) * 5
      inputPath + "/d=" + d + "/h=" + h + "/m5=" + m5
    }


    // 遍历开始时间和结束时间， 将分区目录保存到集合中
    // 注意： 如果目录不存在，则不能加入到集合中， 否则使用spark的外部数据源读取报错终止
    var time: Long = begin
    while(time < end ){
      val path = getPathByTime(time)
      if(fileSystem.exists(new Path(path))){
        partitionSet .+=(path)
      }
      time = time + 5 * 60 * 1000
    }
    // 根据结束时间生成分区目录，保存到set集合中， 即使有重复元素也不要紧，set集合元素都是不重复的
    // 注意： 如果目录不存在，则不能加入到集合中， 否则使用spark的外部数据源读取报错终止
    val path = getPathByTime(end)
    if(fileSystem.exists(new Path(path))){
      partitionSet .+=(path)
    }

    // 将分区目录的集合转换成数组
    partitionSet.toArray
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
    if (beginDF != null && endDF != null) {
      if (beginPartition == endPartition) {
        resultDF = beginDF.filter(s"acctime>='$beginTime' and acctime<='$endTime'")
      } else {
        resultDF = beginDF.filter(s"acctime>='$beginTime'").union(endDF.filter(s"acctime<='$endTime'"))
      }
    } else if (beginDF != null) {
      resultDF = beginDF.filter(s"acctime>='$beginTime'")
    } else if (endDF != null) {
      resultDF = endDF.filter(s"acctime<='$endTime'")
    }

    // ########################################################################
    //   开始和结束的分区以外分区
    // ########################################################################
    val partitionSet = getPartitions(begin, end)
    logger.info("partitionSet:" + partitionSet)
    partitionSet.foreach(p => {
      if (p != beginPartition && p != endPartition) {
        val df = loadFilesToDF(spark, inputPath + p)
        if (resultDF == null) {
          resultDF = df
        } else if (df != null) {
          resultDF = resultDF.union(df)
        }
      }
    })
    resultDF
  }



  def loadFilesToDF(spark: SparkSession, inputPath: String): DataFrame = {
    var df: DataFrame = null
    try {
      df = spark.read.format("orc").load(inputPath).select("srcip","destip","proctype","srcport","destport","domain","url","duration","acctime")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        logger.info(e.getMessage)
      }
    }
    df
  }

}
