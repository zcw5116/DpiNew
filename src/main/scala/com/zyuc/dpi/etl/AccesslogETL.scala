package com.zyuc.dpi.etl

import java.text.SimpleDateFormat
import java.util.Date
import com.alibaba.fastjson.JSONObject
import com.zyuc.dpi.etl.utils.AccessUtil
import com.zyuc.dpi.etl.utils.AccessUtil._
import com.zyuc.dpi.utils.{CommonUtils, FileUtils, JsonValueNotNullException}
import com.zyuc.dpi.utils.FileUtils.renameHDFSDir
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

    var result = ""
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
      val ifDealConflictDir = CommonUtils.getJsonValueByKey(params, "ifDealConflictDir")

      val tryTime = tryTimeStr.toInt
      val coalesceSize = coalesceSizeStr.toInt

      result = "app:" + appName + ", tryTime:" + tryTime

      var begin = new Date().getTime

      val sqlContext = parentContext.newSession()
      sqlContext.sql("use " + hiveDb)

      val batchID = appName.substring(appName.lastIndexOf("_") + 1)
      if(batchID.length<1){
        result = result + s", the length of batchID must gt 0"
        return result
      }

      val partitions = "hid,d,h,m5"

      def getTemplate: String = {
        var template = ""
        val partitionArray = partitions.split(",")
        for (i <- 0 until partitionArray.length)
          template = template + "/" + partitionArray(i) + "=*"
        template
      }

      val inputLocation = inputPath + "/" + batchID
      val inputDoingLocation = inputPath + "/" + batchID + "_doing"
      val inputDoneLocation = inputPath + "/" + batchID + "_done"

     // val inputPathExists = fileSystem.exists(new Path(inputLocation))
     // val inputPathDoingExists = fileSystem.exists(new Path(inputDoingLocation))
     // val inputPathDoneExists = fileSystem.exists(new Path(inputDoneLocation))


      /**
        * 将冲突目录合并成一个目录， 目录的后缀为_doing, 根据返回结果判断是否需要处理：
        *
        * @return false-处理冲突目录失败  true-处理冲突目录成功
        */
      /*
      def dealConflictDir(): Tuple2[Boolean, String] = {

        var msg = ""
        var conflictDir = ""
        var ifSuccess: Boolean = true

        var renameResult: Tuple2[Boolean, String] = (false, msg)

        // 存在两个冲突的目录： do目录(未重命名的1分钟目录, 下同)和doing目录
        if (inputPathExists && inputPathDoingExists && !inputPathDoneExists) {

          conflictDir = s"${batchID}|${batchID}_doing"

        }
        // 存在两个冲突的目录： do目录和done目录
        else if (inputPathExists && inputPathDoneExists && !inputPathDoingExists) {

          conflictDir = s"${batchID}|${batchID}_done"
          if (!renameHDFSDir(fileSystem, inputDoneLocation, inputDoingLocation)) {
            ifSuccess = false
            msg = s"rename from ${inputDoneLocation} to ${inputDoingLocation} failed"
          }

        }
        // 存在两个冲突的目录： doing目录和done目录
        else if (inputPathDoingExists && inputPathDoneExists && !inputPathExists) {

          conflictDir = s"${batchID}_doing|${batchID}_done"
          if (!renameHDFSDir(fileSystem, inputDoingLocation, inputLocation) || !renameHDFSDir(fileSystem, inputDoneLocation, inputDoingLocation)) {
            ifSuccess = false
            msg = s"rename from ${inputDoingLocation} to ${inputLocation} failed or rename from ${inputDoingLocation} to ${inputLocation} failed "
          }

        }
        // 存在三个冲突的目录： do目录/doing目录和done目录
        else if (inputPathDoingExists && inputPathDoneExists && inputPathExists) {

          // 1. 先将doing目录下的文件移动到do目录下, 删除doing目录
          // 2. 将done目录重命名为doing目录

          conflictDir = s"${batchID}|${batchID}_doing|${batchID}_done"

          if (!mvFiles(fileSystem, inputDoingLocation, inputLocation)) {
            ifSuccess = false
            msg = s"move files from ${inputDoingLocation} to ${inputLocation} failed"
          }

          if (!renameHDFSDir(fileSystem, inputDoneLocation, inputDoingLocation)) {
            ifSuccess = false
            msg = s"rename from ${inputDoneLocation} to ${inputDoingLocation} failed"
          }

        }
        // 没有冲突的目录
        else {
          conflictDir = "no conflictDir"
          ifSuccess = false
        }

        msg = s"conflictDir: ${conflictDir}, " + msg

        // 将do目录下的文件移动到doing目录下
        if (ifSuccess) {
          if (!mvFiles(fileSystem, inputLocation, inputDoingLocation)) {
            msg = msg + " move files from ${inputLocation} to ${inputDoingLocation} failed"
            ifSuccess = false
          }
        }

        (ifSuccess, msg)
      }
      */

      /**
        * 1. 判断目录是否冲突， 如果冲突，根据ifDealConflictDir判断是否处理冲突
        * 2. 如果目录不冲突, 判断是否存在待处理的目录
        * 3. 如果最终存在待处理的doing目录， 返回true
        * @return
        */
      /*
      def processDir(): Tuple2[Boolean, String] = {

        var ifDeal = false
        var msg = ""
        // 判断是否有冲突的目录
        if ((inputPathExists && inputPathDoingExists) || (inputPathExists && inputPathDoneExists) || (inputPathDoingExists && inputPathDoneExists)) {
          // 是否处理冲突目录
          if (ifDealConflictDir != "0") {
           val dealResult = dealConflictDir()
            if(dealResult._1){
              ifDeal = true
            }
            msg = dealResult._2
          } else{
            msg = " no deal with conflictDir"
          }

        } else { // 不存在冲突目录
          if (inputPathExists) {
            if (renameHDFSDir(fileSystem, inputLocation, inputDoingLocation)) {
              ifDeal = true
            } else {
              msg = s"rename $inputLocation to $inputDoingLocation failed"
            }
          }
          else if (inputPathDoingExists && tryTime >= 1) {
            ifDeal = true
          } else {
            msg = s"$inputLocation not exists "
          }
        }
        (ifDeal, msg)
      }


      // 对目录重命名等, 需要判断目录是否冲突
      val processResult = processDir()
      val processMsg = processResult._2
      val ifProcessSucc = processResult._1
      logger.info(s"[$appName], ${processMsg}")

      if(!ifProcessSucc){
        result = result + s" ${processMsg}"
        return result
      }
*/

      // 处理数据目录
      val dealDirResult = AccessUtil.dealDir(fileSystem, inputPath, batchID, tryTime, ifDealConflictDir)
      logger.info("[" + appName + "] 处理数据目录： 是否继续执行-" + dealDirResult._1 + ", 处理消息-" + dealDirResult._2)

      // 是否执行后续操作， 不执行返回
      if(!dealDirResult._1){
        result = result + ", " + dealDirResult._2
        return result
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

          val preHourPartNum = if (partitionNum / 2 == 0) 1 else partitionNum / 2

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
        result = "[" + params + "]-" + jsonE.getMessage
        throw new JsonValueNotNullException("[" + params + "]-" + jsonE.getMessage)
        null
      }
      case e: Exception => {
        logger.error("[" + params + "]-" + e.getMessage)
        e.printStackTrace()
        result = "[" + params + "]-" + e.getMessage
        throw new Exception("[" + params + "]-" + e.getMessage)
        null
      }
    }

  }


}
