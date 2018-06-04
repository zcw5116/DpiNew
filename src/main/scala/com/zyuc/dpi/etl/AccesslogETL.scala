package com.zyuc.dpi.etl

import java.text.SimpleDateFormat
import java.util.Date
import com.alibaba.fastjson.JSONObject
import com.zyuc.dpi.etl.utils.AccessUtil
import com.zyuc.dpi.utils.{CommonUtils, FileUtils, JsonValueNotNullException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import scala.collection.mutable

/**
  * Created by zhoucw on 下午10:01.
  */
object AccesslogETL {

  val logger = Logger.getLogger("accessETL")

  def main(args: Array[String]): Unit = {
    val spark = new sql.SparkSession.Builder().appName("test_201803151202").master("local[2]").enableHiveSupport().getOrCreate()
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

    // doJob(sqlContext, fileSystem, appName, inputPath, outputPath, coalesceSize,
    //   loadTime, accessTable, ifRefreshPartiton, tryTime)

  }

  /**
    *
    * @param parentContext
    * @param fileSystem
    * @param params
    * @return
    */
  @throws(classOf[Exception])
  def doJob(parentContext: SQLContext, fileSystem: FileSystem, hiveDb: String, params: JSONObject): String = {

    try {
      var info = ""
      val appName = CommonUtils.getJsonValueByKey(params, "appName")
      val inputPath = CommonUtils.getJsonValueByKey(params, "inputPath")
      val outputPath = CommonUtils.getJsonValueByKey(params, "outputPath")
      val coalesceSizeStr = CommonUtils.getJsonValueByKey(params, "coalesceSize")
      val accessTable = CommonUtils.getJsonValueByKey(params, "accessTable")
      val ifRefreshPartiton = CommonUtils.getJsonValueByKey(params, "ifRefreshPartiton")
      val tryTimeStr = CommonUtils.getJsonValueByKey(params, "tryTime")
      val loadTime = CommonUtils.getJsonValueByKey(params, "loadTime")

      val tryTime = tryTimeStr.toInt
      val coalesceSize = coalesceSizeStr.toInt

      var result = "app:" + appName + ", tryTime:" + tryTime

      var begin = new Date().getTime

      val sqlContext = parentContext.newSession()
      sqlContext.sql("use " + hiveDb)

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

      val sdf = new SimpleDateFormat("yyyyMMddHHmm")
      val curTime = sdf.parse(loadTime)
      val targetSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val preHourTime = targetSdf.format(curTime.getTime() - 2 * 60 * 60 * 1000)
      val curHourTime = targetSdf.format(curTime.getTime() - 1 * 60 * 60 * 1000)

      var resultDF: DataFrame = null

      fileSystem.globStatus(new Path(inputDoingLocation + "/*")).foreach(p => {
        val hLoc = p.getPath.toString
        //val partitionSize = AccessUtil.getPartitionSize(hLoc.substring(hLoc.lastIndexOf("/") + 1))
        val partitionNum = FileUtils.computePartitionNum(fileSystem, hLoc, coalesceSize)
        logger.info("hLoc:" + hLoc + ", partitionNum:" + partitionNum)
        if (partitionNum > 0) {

          val hRowRdd = sqlContext.sparkContext.textFile(hLoc).
            map(x => AccessUtil.parse(x)).filter(_.length != 1)

          val hDF = sqlContext.createDataFrame(hRowRdd, AccessUtil.struct)

          val curHourDF = hDF.filter(s"acctime>='$curHourTime'")
          val preHourDF = hDF.filter(s"acctime>'$preHourTime' and acctime<'$curHourTime' ")

          val preHourPartNum = if (partitionNum / 3 == 0) 1 else partitionNum / 3

          val newDF = curHourDF.coalesce(partitionNum).union(preHourDF.coalesce(preHourPartNum))

          if (resultDF != null) {
            resultDF = resultDF.union(newDF)
          } else {
            resultDF = newDF
          }

        }

      })


      resultDF.write.mode(SaveMode.Overwrite).format("orc")
        .partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + batchID)
      val convertTime = new Date().getTime - begin
      logger.info("[" + appName + "] cost time：" + convertTime)


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
        return result + " success ," + "convertTime:#" + convertTime + "#,moveTime:#" + moveTime + "#"
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

    } catch {
      case jsonE: JsonValueNotNullException => {
        logger.error("[" + params + "]-" + jsonE.getMessage)
        throw new JsonValueNotNullException("[" + params + "]-" + jsonE.getMessage)
        null
      }
      case e: Exception => {
        logger.error("[" + params + "]-" + e.getMessage)
        e.printStackTrace()
        throw new Exception("[" + params + "]-" + e.getMessage)
        null
      }
    }

  }

}
