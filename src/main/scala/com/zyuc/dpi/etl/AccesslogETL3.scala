package com.zyuc.dpi.etl

import java.util.Date

import com.zyuc.dpi.etl.utils.AccessUtil
import com.zyuc.dpi.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
  * Created by zhoucw on 下午10:01.
  * @deprecated
  */
object AccesslogETL3 {
  def main(args: Array[String]): Unit = {
    val spark = new sql.SparkSession.Builder().appName("test_201803151201").master("local[2]").enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    sqlContext.sql("use dpi")
    val appName = sc.getConf.get("spark.app.name") //"name_201711091337" //
    val inputPath = sc.getConf.get("spark.app.inputPath", "hdfs://spark123:8020/tmp/input/accesslog/")
    val outputPath = sc.getConf.get("spark.app.outputPath", "hdfs://spark123:8020/tmp/output/accesslog1/")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size", "1").toInt
    val accessTable = sc.getConf.get("spark.app.accessTable", "test")
    val ifRefreshPartiton = sc.getConf.get("spark.app.ifRefreshPartiton", "0") // 是否刷新分区, 0-不刷新, 1-刷新
    val tryTime = 1
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    doJob(spark, fileSystem, appName, inputPath, outputPath,
      coalesceSize, accessTable, ifRefreshPartiton, tryTime)

  }

  /**
    *
    * @param parentSpark
    * @param fileSystem
    * @param appName           App名字, 格式:${name}_批次目录, 如 201803221501-g1
    * @param inputPath
    * @param outputPath
    * @param coalesceSize      收敛大小，单位M
    * @param accessTable       清洗后外部表
    * @param ifRefreshPartiton 是否刷新分区: 0-不刷新, 1-刷新
    * @param tryTime           重试次数
    * @return
    */
  @throws(classOf[Exception])
  def doJob(parentSpark: SparkSession, fileSystem: FileSystem, appName: String,
            inputPath: String, outputPath: String, coalesceSize: Int, accessTable: String,
            ifRefreshPartiton: String, tryTime: Int): String = {

    try {
      var result = "app:" + appName + ", tryTime:" + tryTime
      val logger = Logger.getLogger("org")
      var begin = new Date().getTime

      val spark = parentSpark.newSession()

      val batchID = appName.substring(appName.lastIndexOf("_") + 1)

      val partitions = "hid,d,h,m5"

      def getTemplate: String = {
        var template = ""
        val partitionArray = partitions.split(",")
        for (i <- 0 until partitionArray.length)
          template = template + "/" + partitionArray(i) + "=*"
        template
      }

      val inputLocation = inputPath + batchID

      val inputDoingLocation = inputPath + "/" + batchID + "_doing"

      val dirExists = fileSystem.exists(new Path(inputLocation))
      if (!dirExists && tryTime == 0) {
        logger.info(s"[$appName] $inputLocation not exists")
        result = result + s" $inputLocation not exists"
        return result
      } else if (!dirExists && tryTime > 0) {
        if (!fileSystem.exists(new Path(inputDoingLocation))) {
          logger.info(s"[$appName] $inputDoingLocation not exists")
          result = result + s" $inputDoingLocation not exists"
          return result
        }
      } else {
        val isDoingRenamed = FileUtils.renameHDFSDir(fileSystem, inputLocation, inputDoingLocation)
        if (!isDoingRenamed) {
          logger.info(s"[$appName] $inputLocation move to $inputDoingLocation failed")
          result = result + s" $inputLocation move to $inputDoingLocation failed"
          return result
        }
        logger.info(s"[$appName] $inputLocation move to $inputDoingLocation success")
      }

      var coalesceNum = FileUtils.computePartitionNum(fileSystem, inputDoingLocation, coalesceSize)
      logger.info(s"[$appName ] coalesceNum $coalesceNum")

      val accRowRdd = spark.sparkContext.textFile(inputDoingLocation).
        map(x => AccessUtil.parse(x)).filter(_.length != 1)

      val accDF = spark.createDataFrame(accRowRdd, AccessUtil.struct)
      accDF.coalesce(coalesceNum).write.mode(SaveMode.Append).format("orc")
        .partitionBy(partitions.split(","): _*).save(outputPath + "data/")
      val convertTime = new Date().getTime - begin
      logger.info("[" + appName + "] 数据转换用时：" + convertTime)
      val inputDoneLocation = inputPath + "/" + batchID + "_done"
      val isDoneRenamed = FileUtils.renameHDFSDir(fileSystem, inputDoingLocation, inputDoneLocation)

      if (!isDoneRenamed) {
        logger.info(s"[$appName] $inputDoingLocation move to $inputDoneLocation failed")
        result = result + " " + s"[$appName] $inputDoingLocation move to $inputDoneLocation failed"
        return result
      }
      logger.info(s"[$appName]  $inputDoingLocation move to $inputDoneLocation success")

      result + " success," + "convertTime:#" + convertTime

    } catch {
      case e: Exception => {
        e.printStackTrace()
        e.getMessage
      }
    }
  }

}
