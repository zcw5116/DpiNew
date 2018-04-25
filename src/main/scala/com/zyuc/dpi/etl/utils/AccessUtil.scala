package com.zyuc.dpi.etl.utils

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import java.util.Base64

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
    StructField("isbase", StringType),
    StructField("duration", StringType),
    StructField("acctime", StringType),

    StructField("d", StringType),
    StructField("h", StringType),
    StructField("m5", StringType)
  ))

  /**
    * from unix time to yyyyMMddHHmmss
    * @param time
    * @return   tuple4(time,d,h,m5)
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
    * @return      Row
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
      var isbase = "1"
      try {
        val urlBytes = Base64.getDecoder.decode(arr(7))
      } catch {
        case e: Exception => {
          //e.printStackTrace()
          isbase = "0"
        }
      }
      Row(houseid, arr(1), arr(2), arr(3), arr(4), arr(5), domain, url, isbase, arr(8), timeTuple._1, timeTuple._2, timeTuple._3, timeTuple._4)
    } catch {
      case e: Exception => {
        val houseid = -1
        Row(houseid, line, "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getTime("1521172762"))


    val asBytes = Base64.getDecoder.decode("aHR0cDovL2J4ZnNmcy5zeGwubWUvdGVtcC9iM2Y2NDM1YmQzNjc4MzRhMzUwNGE5YzYxMDdmMmRjYS5waHA/bT0x")

    System.out.println(new String(asBytes, "utf-8"))
  }

}
