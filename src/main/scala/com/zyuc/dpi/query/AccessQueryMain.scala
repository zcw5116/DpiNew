package com.zyuc.dpi.query

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.zyuc.dpi.query.AccessOrcLogQuery.{getQueryDF, logger}
import com.zyuc.dpi.utils.{CommonUtils, JsonValueNotNullException}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoucw on 下午7:36.
  */
object AccessQueryMain {
  def doJob(parentSpark: SparkSession, fileSystem: FileSystem, params: JSONObject):String = {
    var info = ""
    try{

      val spark = parentSpark.newSession()

      val appName = CommonUtils.getJsonValueByKey(params, "appName")
      val batchid = CommonUtils.getJsonValueByKey(params, "batchid")
      val hid = CommonUtils.getJsonValueByKey(params, "hid")
      val beginTime = CommonUtils.getJsonValueByKey(params, "beginTime")
      val endTime = CommonUtils.getJsonValueByKey(params, "endTime")
      val inputPath = CommonUtils.getJsonValueByKey(params, "inputPath")

      logger.info("Orc query start..")
      var begin = new Date().getTime
      val df = AccessOrcLogQuery.getQueryDF(spark, inputPath, beginTime, endTime)
      df.write.format("csv").mode(SaveMode.Overwrite).options(Map("sep"->",")).save("/tmp/zhou/" + batchid)
      val orcCount = df.count()
      val costTime = new Date().getTime - begin
      info = "#" + batchid + "#" + hid + "#queryS:" + beginTime + "#queryE" +
        endTime + "#" + "orcTime:" + costTime + "#" + "orcCount:" + orcCount + "#"
      logger.info("Orc query end..")



    } catch {
      case jsonE:JsonValueNotNullException => {
        info = "[" + params + "]-" + jsonE.getMessage
        logger.error(info)
        throw new JsonValueNotNullException(info)
      }
      case e: Exception => {
        info = "[" + params + "]-" + e.getMessage
        logger.error(info)
        throw new JsonValueNotNullException(info)
      }
    }
    info
  }

}
