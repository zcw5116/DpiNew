package com.zyuc.dpi.service

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.typesafe.config.Config
import com.zyuc.dpi.etl.AccesslogETL
import com.zyuc.dpi.query.{AccessOrcLogQuery, AccessQueryMain}
import com.zyuc.dpi.utils.{CommonUtils, JsonValueNotNullException}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

/**
  * Created by zhoucw on 下午3:35.
  */
object ServerLine {
  def access(spark: SparkSession, fileSystem: FileSystem, config: Config, params: JSONObject): String = {

    var serverInfo = "服务异常"

    try {

      val serverLine = CommonUtils.getJsonValueByKey(params, "serverLine")

      val sqlContext = spark.sqlContext
      val hiveDb = config.getString("hadoopCfg.hivedb")

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val begin = new Date().getTime

      if (serverLine == "test123") {

        spark.newSession().read.format("json").load("/hadoop/zcw/tmp/zips.json").show

      } else if (serverLine == "accessM5ETL") {

        serverInfo = AccesslogETL.doJob(sqlContext, fileSystem, hiveDb, params)

      } else if (serverLine == "accessQuery") {

        serverInfo = AccessQueryMain.doJob(spark, fileSystem, params)

      }

      val end = new Date().getTime
      serverInfo = serverInfo + "s=" + sdf.format(begin) + "=e=" + sdf.format(end) + "=t=" + (end - begin) + "="
      serverInfo

    } catch {
      // Json parse error
      case jsonE: JsonValueNotNullException => {

        serverInfo = serverInfo + jsonE.getMessage
        serverInfo

      }

      case e: Exception => {

        serverInfo = serverInfo + e.getMessage
        serverInfo

      }
    }
  }
}
