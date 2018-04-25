package com.zyuc.test

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.util.matching.Regex

/**
  * Created by zhoucw on 18-4-24 上午10:03.
  */
object Spark2Test {
  def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder().enableHiveSupport().appName("test").master("local[3]").getOrCreate()
    val sc = spark.sparkContext

    val domainInfo = sc.textFile("/hadoop/basic/domainInfo.txt").
      map(x=>x.split("\\t")).filter(_.length==3).map(x=>(x(0), x(1)))

    val inDomain = domainInfo.filter(_._1=="in").map(_._2).collect()
    val areaDomain = domainInfo.filter(_._1=="area").map(_._2).collect()
    val countryDomain = domainInfo.filter(_._1=="country").map(_._2).collect()

    val bd_inDomain = sc.broadcast(inDomain)
    val bd_areaDomain = sc.broadcast(areaDomain)
    val bd_countryDomain = sc.broadcast(countryDomain)

    /**
      * UDF函数
      * 功能： 1. 判断域名是否合法
      *       2. 根据域名获取顶级域名
      */
    val udf_domain = udf({
      val regExpr = "^[\\w\\-:.]+$"
      var topDomain = ""
      val pattern = regExpr.r.pattern
       println("topDomain:" + topDomain)
      (domain: String) => pattern.matcher(domain.trim).matches() match {
        case true =>
          println("topDomain:" + "abc")
          val arrDomain = domain.split("\\.")
          val len = arrDomain.length
          var last_1 = "-1" // 最后一位
          var last_2 = "-1" // 倒数第二位
          var last_3 = "-1" // 倒数第三位

          if(len>2){
            last_1 = arrDomain(len-1)
            last_2 = arrDomain(len-2)
            last_3 = arrDomain(len-3)
          } else if(len>1){
          last_1 = arrDomain(len-1)
          last_2 = arrDomain(len-2)
        }
          if(inDomain.contains(last_1)){
            topDomain = last_2 + "." + last_1
          }else if(countryDomain.contains(last_1)){
            if(inDomain.contains(last_2) || areaDomain.contains(last_2+"." + last_1)){ // sina.com.cn
              topDomain = last_3 + "." + last_2 + "." + last_1
            }else{
              topDomain = last_2 + "." + last_1
            }
          }else{
            topDomain = ""
          }
          topDomain
        case _ => {
          "-1"
        }
      }
    })

    import spark.implicits._
    val df = sc.makeRDD(List("www.baidu.com", "3gimg.qq.abc", "abc.sina.com.cn", "hello.123.js.cn", "3gaa.qq.com#")).toDF("domain")
    df.select(udf_domain($"domain")).show()

  }
}
