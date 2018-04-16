package com.zyuc.dpi.query

import java.text.SimpleDateFormat
import java.util.Date
import com.alibaba.fastjson.JSONObject
import com.zyuc.dpi.utils.{CommonUtils, JsonValueNotNullException}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoucw on 下午7:36.
  */
object AccessQueryMain {

  val logger = Logger.getLogger("AccessQueryMain")

  def doJob(parentSpark: SparkSession, fileSystem: FileSystem, params: JSONObject): String = {
    var info = ""
    try {

      val spark = parentSpark.newSession()
      val appName = CommonUtils.getJsonValueByKey(params, "appName")
      val batchid = CommonUtils.getJsonValueByKey(params, "batchid")
      val hid = CommonUtils.getJsonValueByKey(params, "hid")
      val beginTime = CommonUtils.getJsonValueByKey(params, "beginTime")
      val endTime = CommonUtils.getJsonValueByKey(params, "endTime")
      val inputOrcPath = CommonUtils.getJsonValueByKey(params, "inputPath")
      val inputTextPath = CommonUtils.getJsonValueByKey(params, "inputTextPath")
      val recordsNumPerPartiton = CommonUtils.getJsonValueByKey(params, "recordsNumPerPartiton")
      val textQueryType = CommonUtils.getJsonValueByKey(params, "textQueryType")

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val beginUnix = (sdf.parse(beginTime).getTime / 1000).toString
      val endUnix = (sdf.parse(endTime).getTime / 1000).toString

      logger.info("Orc query start..")
      var begin = new Date().getTime
      val ordDF = AccessOrcLogQuery.getQueryDF(spark, inputOrcPath, beginTime, endTime)
      val orcCount = ordDF.count()
      val partitionNum = if(orcCount/recordsNumPerPartiton.toInt == 0 ) 1 else orcCount/recordsNumPerPartiton.toInt
      ordDF.repartition(partitionNum.toInt).write.format("csv").mode(SaveMode.Overwrite).options(Map("sep" -> ",")).save("/tmp/zhou/" + batchid + "/orc")
      var costTime = new Date().getTime - begin
      info = "#" + batchid + "#" + hid + "#queryS:" + beginTime + "#queryE:" +
        endTime + "#" + "orcTime:" + costTime + "#" + "orcCount:" + orcCount + "#"
      logger.info("Orc query end..")

      if(textQueryType == "count"){
        logger.info("Text count query start..")
        begin = new Date().getTime
        val textDF = AccessTextLogQuery.getQueryDF(spark, inputTextPath, beginUnix, endUnix)
        textDF.repartition(partitionNum.toInt).write.format("csv").mode(SaveMode.Overwrite).options(Map("sep" -> ",")).save("/tmp/zhou/" + batchid + "/text")
        val textCount = textDF.count()
        costTime = new Date().getTime - begin
        info = info + "textTime:" + costTime + "#" + "textCount:" + textCount + "#"
        logger.info("Text count query end..")
      }else if(textQueryType == "performance"){
        logger.info("Text performance query start..")
        begin = new Date().getTime
        val textDF = AccessTextLogQuery.getQueryDF(spark, inputTextPath, beginUnix, endUnix)
        textDF.repartition(1).write.format("csv").mode(SaveMode.Overwrite).options(Map("sep" -> ",")).save("/tmp/zhou/" + batchid + "/text")
        val textCount = textDF.count()
        costTime = new Date().getTime - begin
        info = info + "textTime:" + costTime + "#" + "textCount:" + textCount + "#"
        logger.info("Text performance query end..")
      }
      else {
        logger.info("Text query none..")
      }
    } catch {

      case jsonE: JsonValueNotNullException => {

        info = "[" + params + "]-" + jsonE.getMessage
        logger.error(info)
        throw new JsonValueNotNullException(info)

      }
      case e: Exception => {

        info = "[" + params + "]-" + e.getMessage
        logger.error(info)
        throw new Exception(info)

      }
    }
    info
  }

}
