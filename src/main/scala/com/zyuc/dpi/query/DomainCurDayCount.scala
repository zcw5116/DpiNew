package com.zyuc.dpi.query

import java.io.ByteArrayInputStream
import java.util.Date
import java.text.SimpleDateFormat

import org.apache.log4j.Logger
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.hadoop.io.IOUtils

import com.zyuc.dpi.stat.AccessLogStatHour.registerTopDomainUDF

/**
  * Created by csq, 2018-07-18 15:13:20
  *
  */
object DomainCurDayCount {
  val logger = Logger.getLogger("domainCurDayCount")

  def main(args: Array[String]): Unit = {
    val nowDate = getTimeString("yyyyMMdd", 0)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val curSec = getTimeString("yyyy-MM-dd HH:mm:ss")

    val batchID   = sc.getConf.get("spark.app.batchid",   curSec)
    val queryDate = sc.getConf.get("spark.app.queryDate", nowDate)
    val domain    = sc.getConf.get("spark.app.domain",    null)
    var topDomainFlag     = sc.getConf.get("spark.app.topDomainFlag", "0")
    val topDomainInfoFile = sc.getConf.get("spark.app.topDomainInfoFile", "/hadoop/basic/domainInfo.txt")
    val countAccessFlag   = sc.getConf.get("spark.app.countAccessFlag",   "1")
    val inputPath_Stat    = sc.getConf.get("spark.app.statParentPath",   "/hadoop/accesslog_stat/hour/out")
    val inputPath_Access  = sc.getConf.get("spark.app.accessParentPath", "/hadoop/accesslog")
    val outputPath        = sc.getConf.get("spark.app.outputParentPath", "/hadoop/accesslog_query/hour/out")

    // domain not null
    if (domain == null || domain.length() == 0) {
      logger.error(s"para error, domain($domain) can not be null!")
      return
    }
    //topDomainFlag 0 or 1
    if (topDomainFlag != null && topDomainFlag.length() != 0) {
      if (! List("0", "1").contains(topDomainFlag)) {
        logger.error(s"para error, topdomainflag($topDomainFlag) should be in (0, 1)!")
      }
    } else {
      topDomainFlag = "0"
    }

    val params: Map[String, String] = Map(
      "batchID"          -> batchID,
      "queryDate"        -> queryDate,
      "domain"           -> domain,
      "topDomainFlag"    -> topDomainFlag,
      "countAccessFlag"  -> countAccessFlag,
      "inputPath_Stat"   -> inputPath_Stat,
      "inputPath_Access" -> inputPath_Access,
      "outputPath"       -> outputPath,
      "topDomainFile"    -> topDomainInfoFile
    )
    doCountDomain(spark, params, fileSystem)
  }

  /** doCountDomain
    *
    * @author csq
    * @param spark object of org.apache.spark.sql.SparkSession
    * @param params parameters in map format
    * @param fileSystem object of org.apache.hadoop.fs.FileSystem
    * @return null
    */
  def doCountDomain(spark: SparkSession, params: Map[String, String], fileSystem: FileSystem): Unit = {
    val batchID   = params("batchID")
    val queryDate = params("queryDate")
    val domain    = params("domain")
    val topDomainFlag    = params("topDomainFlag")
    val countAccessFlag  = params("countAccessFlag")
    val inputPath_Stat   = params("inputPath_Stat")
    val inputPath_Access = params("inputPath_Access")
    val outputPath       = params("outputPath") +"/DomainCurDayCount.csv"
    val topDomainFile    = params("topDomainFile")

    // d=yymmdd
    val queryPathDate = queryDate.substring(2, 8)
    val curDay = getTimeString("yyyyMMdd")
    val (lastHour, curHour) = (getTimeString("HH", -3600 * 1000), getTimeString("HH", 0))
    val curSec = getTimeString("yyyy-MM-dd HH:mm:ss")

    val sql_Stat = topDomainFlag match {
      case "0" => s"""
                     |select '${batchID}' as batchID, '${domain}' as domain, '${curSec}' as curSec, sum(times) as times
                     |from domaintable
                     |where domain = '${domain}'
                   """.stripMargin
      case "1" => s"""
                     |select '${batchID}' as batchID, '${domain}' as domain, '${curSec}' as curSec, sum(times) as times
                     |from domaintable
                     |where topdomain = '${domain}'
                   """.stripMargin
    }
    val Sql_Access = topDomainFlag match {
      case "0" => s"""
                     |select '${batchID}' as batchID, '${domain}' as domain, '${curSec}' as curSec, count(1) as times
                     |from accesslogtable
                     |where domain = '${domain}'
                   """.stripMargin
      case "1" => s"""
                     |select '${batchID}' as batchID, '${domain}' as domain, '${curSec}' as curSec, count(1) as times
                     |from accesslogtable
                     |where udf_topDomain(domain) = '${domain}'
                   """.stripMargin
    }

    // 活跃资源统计结果内查询
    var str = "hour".r findFirstIn inputPath_Stat
    if (str != null ) {
      val inputLoc_Stat = inputPath_Stat + "/domain/hid=*/d=" +queryPathDate +"/h=*/*.orc"
      doCountDomain_StatHour(spark, fileSystem, outputPath, domain, inputLoc_Stat, sql_Stat)
    } else {
      val inputLoc_Stat = inputPath_Stat + "/domain/hid=*/d=" +queryPathDate +"/*.csv"
      doCountDomain_StatDay(spark, fileSystem, outputPath, domain, inputLoc_Stat, sql_Stat)
    }

    // 本小时和前小时访问日志内查询
    val inputLoc_Access1 = inputPath_Access +"/" +curDay +lastHour +"*done/*/*.txt"
    val inputLoc_Access2 = inputPath_Access +"/" +curDay +curHour +"*done/*/*.txt"
    if (curDay.equals(queryDate) && countAccessFlag == "1") {
      if (topDomainFlag == "1") {
        // 广播域名基础数据
        registerTopDomainUDF(spark, topDomainFile)
      }

      doCountDomain_Access(spark, fileSystem, outputPath, domain, inputLoc_Access1, inputLoc_Access2, Sql_Access)
    }
  }

  /** Get total num of specified domain from statistics of access log
    * @author csq
    * @param spark object of org.apache.spark.sql.SparkSession
    * @param fileSystem object of org.apache.hadoop.fs.FileSystem
    * @param outputPath path which will save result
    * @param domain domain to summary
    * @param inputLoc_Access1 location of access log file of last hour
    * @param inputLoc_Access2 location of access log file of current hour
    * @param Sql_Access spark sql for getting count of domain
    * @return null
    */
  def doCountDomain_Access(spark: SparkSession, fileSystem: FileSystem, outputPath: String,
                           domain: String, inputLoc_Access1: String, inputLoc_Access2: String, Sql_Access: String): Unit = {
    var isDirEmpty = true
    var rddAll: RDD[(String)] = spark.sparkContext.makeRDD(List())
    for (inputLoc <- List(inputLoc_Access1, inputLoc_Access2)) {
      isDirEmpty = true
      val fileIterator = fileSystem.globStatus(new Path(inputLoc)).toIterator
      while (fileIterator.hasNext && isDirEmpty) {
        val file = fileIterator.next()
        if (file.getLen > 0) {
          isDirEmpty = false
        }
      }

      if (!isDirEmpty) {
        val rdd = spark.sparkContext.textFile(inputLoc).map(x=>x.split("\\|")).filter(_.length == 9).map(x => x(6))
        rddAll = rddAll.union(rdd)
      }
    }

    import spark.implicits._
    val df = rddAll.toDF("domain")
    df.createOrReplaceTempView("accesslogtable")

    val resultDF = spark.sql(Sql_Access).selectExpr("batchID", "domain", "curSec", "cast (times as long)")
    saveToTextFile(fileSystem, outputPath ,resultDF)
  }

  /** Get total num of specified domain from day statistics of access log
    * @author csq
    * @param spark object of org.apache.spark.sql.SparkSession
    * @param fileSystem object of org.apache.hadoop.fs.FileSystem
    * @param outputPath path which will save result
    * @param domain domain to summary
    * @param inputLoc_Stat location of statistics file
    * @param sql_Stat spark sql for getting count of domain
    * @return null
    */
  def doCountDomain_StatDay(spark: SparkSession, fileSystem: FileSystem, outputPath: String,
                         domain: String, inputLoc_Stat: String, sql_Stat: String): Unit = {
    var isDirEmpty = true
    val fileIterator = fileSystem.globStatus(new Path(inputLoc_Stat)).toIterator
    while (fileIterator.hasNext && isDirEmpty) {
      val file = fileIterator.next()
      if (file.getLen > 0) {
        isDirEmpty = false
      }
    }

    if (!isDirEmpty) {
      // 没有顶级域名的都是ip格式的,过滤掉
      val rdd = spark.sparkContext.textFile(inputLoc_Stat).map(x=>x.split("\\t")).filter(_.length == 7).map(x => (x(1), x(4), x(6)))
      import spark.implicits._
      val df = rdd.toDF("domain", "times", "topdomain")
      df.createOrReplaceTempView("domaintable")

      // dataframe 转 string后 存入指定文件
      val resultDF = spark.sql(sql_Stat).selectExpr("batchID", "domain", "curSec", "cast (times as long)")
      saveToTextFile(fileSystem, outputPath ,resultDF)
    } else {
      logger.error(s"location{$inputLoc_Stat} is empty")
    }
  }

  /** Get total num of specified domain from hour statistics of access log
    * @author csq
    * @param spark object of org.apache.spark.sql.SparkSession
    * @param fileSystem object of org.apache.hadoop.fs.FileSystem
    * @param outputPath path which will save result
    * @param domain domain to summary
    * @param inputLoc_Stat location of statistics file
    * @param sql_Stat spark sql for getting count of domain
    * @return null
    */
  def doCountDomain_StatHour(spark: SparkSession, fileSystem: FileSystem, outputPath: String,
                         domain: String, inputLoc_Stat: String, sql_Stat: String): Unit = {
    var isDirEmpty = true
    val fileIterator = fileSystem.globStatus(new Path(inputLoc_Stat)).toIterator
    while (fileIterator.hasNext && isDirEmpty) {
      val file = fileIterator.next()
      if (file.getLen > 0) {
        isDirEmpty = false
      }
    }

    if (!isDirEmpty) {
      // 没有顶级域名的都是ip格式的,过滤掉
      val rdd = spark.read.format("orc").load(inputLoc_Stat)
      rdd.createOrReplaceTempView("domaintable")

      // dataframe 转 string后 存入指定文件
      val resultDF = spark.sql(sql_Stat).selectExpr("batchID", "domain", "cursec", "cast (times as long)")
      saveToTextFile(fileSystem, outputPath ,resultDF)
    } else {
      logger.error(s"location{$inputLoc_Stat} is empty")
    }
  }


  /** convert dataframe to string and save to specified file
    * @author csq
    * @param fileSystem object of org.apache.hadoop.fs.FileSystem
    * @param outputPath file path which will save result
    * @param resultDF to be converted dataframe
    * @return null
    */
  def saveToTextFile(fileSystem:FileSystem, outputPath:String ,resultDF: DataFrame): Unit = {
    // dataFrame -> String
    var resultStr: String = ""
    val resultList = resultDF.takeAsList(resultDF.count().toInt)
    import scala.collection.JavaConversions._
    for (resultRow <- resultList) {
      resultStr += resultRow.mkString(s"\t") +s"\r\n"
    }
    if (resultStr.length() == 0) {
      logger.warn(s"nothing to write, return")
    }

    // String -> InputStream -> outputPath
    val resultFile: Path = new Path(outputPath)
    if (fileSystem.exists(resultFile)) {
      logger.info(s"file resultFile already exists, append to file ")
    } else {
      logger.info(s"file resultFile does not exists, create file first")
      fileSystem.createNewFile(resultFile)
    }
    val out: FSDataOutputStream = fileSystem.append(resultFile)
    val in: ByteArrayInputStream =  new ByteArrayInputStream(resultStr.getBytes)
    IOUtils.copyBytes(in, out, 4096, true)
  }

  /** Get formatted time string
    * @author csq
    * @param format date format such as "yyyy-MM-dd HH:mm:ss"
    * @return time string in specified format
    */
  def getTimeString(format: String): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    val timeString = dateFormat.format(now)
    timeString
  }

  /** Get formatted time string with shifting
    * @author csq
    * @param format date format such as "yyyy-MM-dd HH:mm:ss"
    * @param shift shifting of microseconds(ms)
    * @return shifed time string in specified format
    */
  def getTimeString(format: String, shift: Long): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    val timeString = dateFormat.format(new Date(now.getTime + shift))
    timeString
  }
}
