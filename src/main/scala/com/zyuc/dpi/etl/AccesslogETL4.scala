package com.zyuc.dpi.etl

import java.text.SimpleDateFormat
import java.util.Date

import com.zyuc.dpi.etl.utils.AccessUtil
import com.zyuc.dpi.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql
import org.apache.spark.sql.{SQLContext, SaveMode}

import scala.collection.mutable

/**
  * Created by zhoucw on 下午10:01.
  */
object AccesslogETL4 {
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
    val loadTime = "2018-03-16 16:00:00"

    doJob(sqlContext, fileSystem, appName, inputPath, outputPath, coalesceSize,
      loadTime, accessTable, ifRefreshPartiton, tryTime)

  }

  /**
    *
    * @param parentContext
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
  def doJob(parentContext: SQLContext, fileSystem: FileSystem, appName: String,
            inputPath: String, outputPath: String, coalesceSize: Int, loadTime:String,
            accessTable: String, ifRefreshPartiton: String, tryTime: Int): String = {

    try{
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

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val endTime = sdf.parse(loadTime)
      val beginTime = sdf.format(endTime.getTime() - 2*60*60*1000)
      val curHourTime = sdf.format(endTime.getTime() - 1*60*60*1000)

      val accRowRdd = sqlContext.sparkContext.textFile(inputDoingLocation).
        map(x => AccessUtil.parse(x)).filter(_.length != 1)//.filter(x=>x.getString(10)>beginTime && x.getString(10)< loadTime)

      val accDF = sqlContext.createDataFrame(accRowRdd, AccessUtil.struct)

      val curHourDF = accDF.filter(s"acctime>='$curHourTime'")
      val preHourDF = accDF.filter(s"acctime>'$beginTime' and acctime<'$curHourTime' ")

      val newDF = curHourDF.coalesce(60).union(preHourDF.coalesce(20))


      newDF.write.mode(SaveMode.Overwrite).format("orc")
        .partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + batchID)
      val convertTime = new Date().getTime - begin
      logger.info("[" + appName + "] 数据转换用时：" + convertTime)

      begin = new Date().getTime
      val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + batchID + getTemplate + "/*.orc"))
      val filePartitions = new mutable.HashSet[String]
      for (i <- 0 until outFiles.length) {
        val nowPath = outFiles(i).getPath.toString
        filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + batchID, "").substring(1))
      }
      FileUtils.moveTempFiles(fileSystem, outputPath, batchID, getTemplate, filePartitions)
      val moveTime = new Date().getTime - begin
      logger.info("[" + appName + "] 数据移动到正式分区用时：" + moveTime)


      val inputDoneLocation = inputPath + "/" + batchID + "_done"
      val isDoneRenamed = FileUtils.renameHDFSDir(fileSystem, inputDoingLocation, inputDoneLocation)

      if (!isDoneRenamed) {
        logger.info(s"[$appName] $inputDoingLocation move to $inputDoneLocation failed")
        result = result + " " + s"[$appName] $inputDoingLocation move to $inputDoneLocation failed"
        return result
      }
      logger.info(s"[$appName]  $inputDoingLocation move to $inputDoneLocation success")

      // 是否刷新分区
      if (ifRefreshPartiton == "0") {
        return result + " success ," + "convertTime:#" + convertTime + "#,moveTime:#" + moveTime+"#"
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
      val refreshTime = new Date().getTime - begin
      logger.info("[" + appName + "] 刷新分区用时：" + refreshTime)

      result + " success," + "convertTime:#" + convertTime + "#,moveTime:#" + moveTime + "#,refreshTime:#" + refreshTime

    }catch {
      case e:Exception =>{
      e.printStackTrace()
      e.getMessage
      }
    }

  }

}
