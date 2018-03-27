package com.zyuc.dpi.etl

import java.util.Date

import com.zyuc.dpi.etl.utils.AccessConveterUtil
import com.zyuc.dpi.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.log4j.Logger

import scala.collection.mutable

/**
  * Created by zhoucw on 下午10:01.
  */
object AccesslogETL {
  def main(args: Array[String]): Unit = {
    val spark = new sql.SparkSession.Builder().enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    sqlContext.sql("use dpi")
    val appName = sc.getConf.get("spark.app.name") //"name_201711091337" //
    val inputPath = sc.getConf.get("spark.app.inputPath", "hdfs://spark123:8020/tmp/input/accesslog/")
    val outputPath = sc.getConf.get("spark.app.outputPath", "hdfs://spark123:8020/tmp/output/accesslog/")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size", "1").toInt
    val accessTable = sc.getConf.get("spark.app.accessTable", "test")
    val ifRefreshPartiton = sc.getConf.get("spark.app.ifRefreshPartiton", "0") // 是否刷新分区, 0-不刷新, 1-刷新

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    doJob(sqlContext, fileSystem, appName, inputPath, outputPath, coalesceSize, accessTable, ifRefreshPartiton, 0)

  }

  /**
    *
    * @param parentContext
    * @param fileSystem
    * @param appName             App名字, 格式:${name}_批次目录, 如 201803221501-g1
    * @param inputPath
    * @param outputPath
    * @param coalesceSize       收敛大小，单位M
    * @param accessTable        清洗后外部表
    * @param ifRefreshPartiton  是否刷新分区
    * @param tryTime            重试次数
    * @return
    */
  @throws(classOf[Exception])
  def doJob(parentContext: SQLContext, fileSystem: FileSystem, appName: String,
            inputPath: String, outputPath: String, coalesceSize: Int, accessTable: String,
            ifRefreshPartiton: String, tryTime:Int) : String = {

    var result = "app:" + appName + ", tryTime:" + tryTime
    val logger = Logger.getLogger("org")
    var begin = new Date().getTime

      val sqlContext = parentContext.newSession()

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
      if(!dirExists && tryTime==0){
        logger.info(s"[$appName] $inputLocation not exists")
        result = result + s" $inputLocation not exists"
        return  result
      } else if(!dirExists && tryTime>0){
        if(!fileSystem.exists(new Path(inputDoingLocation))){
          logger.info(s"[$appName] $inputDoingLocation not exists")
          result = result + s" $inputDoingLocation not exists"
          return  result
        }
      } else {
        val isDoingRenamed = FileUtils.renameHDFSDir(fileSystem, inputLocation, inputDoingLocation)
        if(!isDoingRenamed){
          logger.info(s"[$appName] $inputLocation move to $inputDoingLocation failed")
          result = result + s" $inputLocation move to $inputDoingLocation failed"
          return result
        }
        logger.info(s"[$appName] $inputLocation move to $inputDoingLocation success")
      }

      var coalesceNum = FileUtils.computePartitionNum(fileSystem, inputDoingLocation, coalesceSize)
      logger.info(s"[$appName ] coalesceNum $coalesceNum")

      val accRowRdd = sqlContext.sparkContext.textFile(inputDoingLocation).
        map(x => AccessConveterUtil.parse(x)).filter(_.length != 1)

      val accDF = sqlContext.createDataFrame(accRowRdd, AccessConveterUtil.struct)
      accDF.coalesce(coalesceNum).write.mode(SaveMode.Overwrite).format("orc")
        .partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + batchID)
      logger.info("[" + appName + "] 数据转换用时：" + (new Date().getTime - begin ))

      begin = new Date().getTime
      val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + batchID + getTemplate + "/*.orc"))
      val filePartitions = new mutable.HashSet[String]
      for (i <- 0 until outFiles.length) {
        val nowPath = outFiles(i).getPath.toString
        filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + batchID, "").substring(1))
      }
      FileUtils.moveTempFiles(fileSystem, outputPath, batchID, getTemplate, filePartitions)
      logger.info("[" + appName + "] 数据移动到正式分区用时：" + (new Date().getTime - begin ))


      val inputDoneLocation = inputPath + "/" + batchID + "_done"
      val isDoneRenamed = FileUtils.renameHDFSDir(fileSystem, inputDoingLocation, inputDoneLocation)

      if(!isDoneRenamed){
        logger.info(s"[$appName] $inputDoingLocation move to $inputDoneLocation failed")
        result = result + " " + s"[$appName] $inputDoingLocation move to $inputDoneLocation failed"
        return result
      }
      logger.info(s"[$appName]  $inputDoingLocation move to $inputDoneLocation success")


      // 是否刷新分区
      if (ifRefreshPartiton == "0") {
        return result + " success"
      }

      begin = new Date().getTime
    // 过滤
    //filePartitions.filter(x=>(x.substring(x.indexOf("/d="), x.indexOf("/h=")))>"a")
      filePartitions.foreach(partition => {
        var hid = ""
        var d = ""
        var h = ""
        var m5 = ""
        partition.split("/").map(x => {
          if (x.startsWith("hid=")) {
            hid = x.substring(4)
          }
          if (x.startsWith("d=")) {
            d = x.substring(2)
          }
          if (x.startsWith("h=")) {
            h = x.substring(2)
          }
          if (x.startsWith("m5=")) {
            m5 = x.substring(3)
          }
          null
        })
        if (d.nonEmpty && h.nonEmpty && m5.nonEmpty) {
          val sql = s"alter table ${accessTable} add IF NOT EXISTS partition(hid='$hid', d='$d', h='$h',m5='$m5')"
          sqlContext.sql(sql)
        }
      })
      logger.info("[" + appName + "] 刷新分区用时：" + (new Date().getTime - begin ))

      result + " success"

  }

}
