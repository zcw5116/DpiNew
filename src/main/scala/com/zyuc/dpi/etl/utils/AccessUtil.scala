package com.zyuc.dpi.etl.utils

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import java.util.Base64

import com.zyuc.dpi.utils.ConfigUtil
import com.zyuc.dpi.utils.FileUtils.renameHDFSDir
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

/**
  * Created on 上午3:16.
  */
object AccessUtil {

  val struct = StructType(Array(
    StructField("hid", IntegerType),
    StructField("srcip", StringType),
    StructField("destip", StringType),
    StructField("proctype", StringType),
    StructField("srcport", StringType),

    StructField("destport", StringType),
    StructField("domain", StringType),
    StructField("url", StringType),
    //StructField("isbase", StringType),
    StructField("duration", StringType),
    StructField("acctime", StringType),

    StructField("d", StringType),
    StructField("h", StringType),
    StructField("m5", StringType)
  ))

  /**
    * from unix time to yyyyMMddHHmmss
    *
    * @param time
    * @return tuple4(time,d,h,m5)
    */
  def getTime(time: String): Tuple4[String, String, String, String] = {
    try {
      val targetfdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
      val timeStr = targetfdf.format(time.toLong * 1000)
      val d = timeStr.substring(2, 10).replaceAll("-", "")
      val h = timeStr.substring(11, 13)
      val m5 = timeStr.substring(14, 15) + (timeStr.substring(15, 16).toInt / 5) * 5
      (timeStr, d, h, m5)
    } catch {
      case e: Exception => {
        ("0", "0", "0", "0")
      }
    }
  }


  /**
    *
    * @param line
    * @return Row
    */
  def parse(line: String) = {
    try {
      val arr = line.split("\\|", 10)
      var houseid = 0
      val timeTuple = getTime(arr(9))
      try {
        houseid = arr(0).toInt
      } catch {
        case e: Exception => {
        }
      }

      val domain = arr(6).trim

      var url = arr(7)

      // 对base64位加密的url进行解码
      /*
      var isbase = "1"
      try {
        val urlBytes = Base64.getDecoder.decode(arr(7))
      } catch {
        case e: Exception => {
          //e.printStackTrace()
          isbase = "0"
        }
      }
      */

      Row(houseid, arr(1), arr(2), arr(3), arr(4), arr(5), domain, url, arr(8), timeTuple._1, timeTuple._2, timeTuple._3, timeTuple._4)
    } catch {
      case e: Exception => {
        val houseid = -1
        Row(houseid, line, "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1")
      }
    }
  }

  /**
    * 根据配置文件， 配置每个机房的分区大小
    *
    * @deprecated
    * @param hid
    * @return
    */
  def getPartitionSize(hid: String): Int = {
    val config = ConfigUtil.getConfig("/home/slview/bigdata/app/spark/server/config/partition.conf")
    var partitionSize = config.getString("partitionSize.0")
    try {
      partitionSize = config.getString("partitionSize." + hid)
    } catch {
      case e: Exception => {
        //e.printStackTrace()
      }
    }
    partitionSize.toInt
  }

  /**
    * 将文件移动到目标目录， 子目录为机房id。 如果源目录为空， 做返回false。 删除目录
    *
    * @param fromDir
    * @param toDir
    * @return
    */
  def mvFiles(fileSystem: FileSystem, fromDir: String, toDir: String): Unit = {

    try {

      fileSystem.globStatus(new Path(fromDir + "/*")).foreach(x => {
        val hidDir = x.getPath.getName
        if (!fileSystem.exists(new Path(toDir + "/" + hidDir))) {
          fileSystem.mkdirs(new Path(toDir + "/" + hidDir))
        }
        fileSystem.globStatus(new Path(s"${fromDir}/${hidDir}/*")).foreach(f => {
          val fname = f.getPath.getName
          val newPath = new Path(toDir + "/" + hidDir + "/" + fname)
          fileSystem.rename(f.getPath, newPath)
        })
      })

      val fromSize = fileSystem.getContentSummary(new Path(fromDir)).getLength
      if (fromSize == 0) {
        fileSystem.delete(new Path(fromDir), true)
      }

    } catch {
      case e: Exception => {
        throw new Exception(s"move files from ${fromDir} to ${toDir} failed." + e.getMessage)
      }
    }
  }

  /**
    *
    * @return (是否继续执行, 处理信息)
    */

  /**
    * 处理数据目录
    * 如果存在冲突的目录, 判断是否处理冲突目录 1. 不处理： 直接返回不处理的信息
    *                                    2. 处理： 移动文件, 将多个冲突目录归整为一个doing目录
    * 如果不存在冲突目录, 1. 只有do目录, 重命名为doing目录
    *                 2. 只有doing目录, 且重试次数大于等于1, 返回true
    *                 3. 其他： 返回false
    *
    * @param fileSystem
    * @param inputPath
    * @param batchID
    * @param tryTime
    * @param ifDeal 是否处理冲突的目录  0-不处理  1-处理
    * @return (是否继续执行, 处理信息)
    */
  def dealDir(fileSystem: FileSystem, inputPath: String, batchID: String, tryTime: Int, ifDeal: String): Tuple2[Boolean, String] = {

    val logger = Logger.getLogger("accessETL")

    val inputLocation = inputPath + "/" + batchID
    val inputDoingLocation = inputPath + "/" + batchID + "_doing"
    val inputDoneLocation = inputPath + "/" + batchID + "_done"

    val inputPathExists = fileSystem.exists(new Path(inputLocation))
    val inputPathDoingExists = fileSystem.exists(new Path(inputDoingLocation))
    val inputPathDoneExists = fileSystem.exists(new Path(inputDoneLocation))


    var msg = ""
    var conflictDir = ""
    var ifNext: Boolean = true

    logger.info(s"batchID:${batchID}, conflictFlag:<${inputPathExists}><${inputPathDoingExists}><${inputPathDoneExists}>")

    // 是否存在冲突的目录
    var ifConflict = (inputPathExists, inputPathDoingExists, inputPathDoneExists) match {
      case (true, true, false) => true
      case (true, false, true) => true
      case (false, true, true) => true
      case (true, true, true) => true
      case _ => false
    }

    msg = msg + s" ifConflict:${ifConflict}, [do, doing, done]:[${inputPathExists}, ${inputPathDoingExists}, ${inputPathDoneExists}]"

    // 存在冲突目录, 但是不处理
    if (ifConflict && ifDeal != "1") {
      ifNext = false
      msg = msg + s", ifConflict:${ifConflict}, ifDealConflict:${ifDeal} "
    }
    // 存在冲突目录且处理
    else if (ifConflict && ifDeal == "1") {

      // 将冲突目录归整为do和doing目录
      (inputPathExists, inputPathDoingExists, inputPathDoneExists) match {
        case (true, true, true) => {
          // 1. 先将doing目录下的文件移动到do目录下, 删除doing目录
          // 2. 将done目录重命名为doing目录
          mvFiles(fileSystem, inputDoingLocation, inputLocation)
          renameHDFSDir(fileSystem, inputDoneLocation, inputDoingLocation)
        }
        case (false, true, true) => {
          // 1. 先将doing目录重命名为do目录
          // 2. 将done目录重命名为doing目录
          renameHDFSDir(fileSystem, inputDoingLocation, inputLocation)
          renameHDFSDir(fileSystem, inputDoneLocation, inputDoingLocation)
        }
        case (true, false, true) => {
          // 将done目录重命名为doing目录
          renameHDFSDir(fileSystem, inputDoneLocation, inputDoingLocation)
        }
        case _ => {
          null
        }
      }
      // 将do目录下的文件移动到doing目录下
      mvFiles(fileSystem, inputLocation, inputDoingLocation)
    }
    // 不存在冲突目录
    else {
      if (inputPathExists) {
        renameHDFSDir(fileSystem, inputLocation, inputDoingLocation)
      } else if (inputPathDoingExists && tryTime >= 1) {
        ifNext = true
      } else {
        ifNext = false
        msg = msg + s", ${inputLocation} not exsits."
      }
    }

    (ifNext, msg)
  }

  def main(args: Array[String]): Unit = {
    // println(getTime("1521172762"))/hadoop/.m2/repository/com/typesafe/config/1.2.1/config-1.2.1.jar
    // val asBytes = Base64.getDecoder.decode("aHR0cDovL2J4ZnNmcy5zeGwubWUvdGVtcC9iM2Y2NDM1YmQzNjc4MzRhMzUwNGE5YzYxMDdmMmRjYS5waHA/bT0x")
    // System.out.println(new String(asBytes, "utf-8"))
    val hLoc = "hdfs://cdh-nn-001:8020/hadoop/accesslog/201805101005_doing/1005"
    val hid = hLoc.substring(hLoc.lastIndexOf("/") + 1)
    println(hid)
    println(getPartitionSize(hid))
  }

}
