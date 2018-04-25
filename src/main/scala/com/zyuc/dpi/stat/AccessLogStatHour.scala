package com.zyuc.dpi.stat

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf

/**
  * Created by zhoucw on 18-4-24 上午9:55.
  */
object AccessLogStatHour {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().appName("AccessLogStatHour").master("local[*]").getOrCreate()

    //#############################################
    //  参数接收
    //#############################################
    val sc = spark.sparkContext
    val inputParentPath = sc.getConf.get("spark.app.inputParentPath", "/hadoop/accesslog_etl_all/output/data")
    val hid = sc.getConf.get("spark.app.hid", "1019")
    val hourtime = sc.getConf.get("spark.app.hourtime", "2018042408")
    val houseIpPath = sc.getConf.get("spark.app.houseIpPath", "/hadoop/idcipseginfo/1019.ip")
    val outputParentPath = sc.getConf.get("spark.app.outputParentPath", "/hadoop/accesslog_stat/hour/out")


/*    // spark sql 使用正则表达式有bug, 使用自定义的udf，正则表达式参考原pig程序
    val udf_isDomain = udf({
      val pattern = "^[\\w\\-:.]+$"
      (s: String) => pattern.r.pattern.matcher(s.trim).matches() match {
        case true =>
          1
        case _ => {
          0
        }
      }
    })*/

    //#############################################
    //   广播域名
    //#############################################
    val domainInfo = sc.textFile("/hadoop/basic/domainInfo.txt").
      map(x => x.split("\\t")).filter(_.length == 3).map(x => (x(0), x(1)))
    val inDomain = domainInfo.filter(_._1 == "in").map(_._2).collect()
    val areaDomain = domainInfo.filter(_._1 == "area").map(_._2).collect()
    val countryDomain = domainInfo.filter(_._1 == "country").map(_._2).collect()

    val bd_inDomain = sc.broadcast(inDomain)
    val bd_areaDomain = sc.broadcast(areaDomain)
    val bd_countryDomain = sc.broadcast(countryDomain)

    //#############################################
    //  自定义UDF函数
    //  功能： 1. 判断域名是否合法
    //        2. 根据域名获取顶级域名
    //#############################################
    val bdv_inDomain = bd_inDomain.value
    val bdv_areaDomain = bd_areaDomain.value
    val bdv_countryDomain = bd_countryDomain.value
    val regExpr = "^[\\w\\-:.]+$"
    val pattern = regExpr.r.pattern
    spark.udf.register("udf_isDomain", (domain: String) => {
      var topDomain = ""
      // 首先判断域名是否合法
      pattern.matcher(domain.trim).matches() match {
        case true =>
          val arrDomain = domain.split("\\.")
          val len = arrDomain.length
          var last_1 = "-1" // 最后一位
        var last_2 = "-1" // 倒数第二位
        var last_3 = "-1" // 倒数第三位

          if (len > 2) {
            last_1 = arrDomain(len - 1)
            last_2 = arrDomain(len - 2)
            last_3 = arrDomain(len - 3)
          } else if (len > 1) {
            last_1 = arrDomain(len - 1)
            last_2 = arrDomain(len - 2)
          }
          if (bdv_inDomain.contains(last_1)) { // baidu.com
            topDomain = last_2 + "." + last_1
          } else if (bdv_countryDomain.contains(last_1)) { // abc.cn  abc.js.cn sina.com.cn
            if (bdv_inDomain.contains(last_2) || bdv_areaDomain.contains(last_2 + "." + last_1)) { // abc.js.cn sina.com.cn
              topDomain = last_3 + "." + last_2 + "." + last_1
            } else { // abc.cn
              topDomain = last_2 + "." + last_1
            }
          } else {
            topDomain = ""
          }
          topDomain
        case _ => {
          "-1"
        }
      }
    })



    //#############################################
    //   读取访问日志数据
    //#############################################
    val d = hourtime.substring(2, 8)
    val h = hourtime.substring(8, 10)
    val inputPath = inputParentPath + "/" + "hid=" + hid + "/d=" + d + "/h=" + h
    val accessTable = "accessLog"
    val dataDF = spark.read.format("orc").load(inputPath)
    // 增加是否合法域名的判断
    //dataDF.withColumn("isdomain", udf_isDomain(dataDF.col("domain"))).createOrReplaceTempView(accessTable)
    dataDF.createOrReplaceTempView(accessTable)
    dataDF.printSchema()

    //#############################################
    // 广播IP地址表数据：缓存IP地址信息
    // 注意： 表数据大小不要超过10M, 否则修改参数spark.sql.autoBroadcastJoinThreshold值(默认为10M)
    //#############################################
    val houseIpTable = "houseIp"
    import spark.implicits._
    val houseIpDF = sc.textFile(houseIpPath).toDF("hip")
    val houseIpTmpView = "houseIpTmpView"
    houseIpDF.createOrReplaceTempView(houseIpTmpView)
    spark.sql(
      s"""
         |cache table ${houseIpTable} as
         |select hip from ${houseIpTmpView}
       """.stripMargin)

   //#############################################
   //   关联是否合法ip
   //#############################################
    val accessAndIpSql =
      s"""
         |select a.*, udf_isDomain(a.domain) topdomain,
         |       case when h.hip is null then 0 else 1 end iflegal
         |from ${accessTable} a left join ${houseIpTable} h
         |on( a.destip = h.hip )
       """.stripMargin

    val accessAndIpTable = "accessAndIp"
    spark.sql(accessAndIpSql).createOrReplaceTempView(accessAndIpTable)

    //#############################################
    //   1. 违法访问日志
    //#############################################
    val illegalLogSql =
      s"""
         |select distinct '$hid' hid, domain, destip
         |from ${accessAndIpTable}
         |where iflegal = 0 and topdomain!='-1'
       """.stripMargin
    val illegalLogPath = outputParentPath + "/illegLog/hid=" + hid + "/d=" + d + "/h=" +h
    spark.sql(illegalLogSql).repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(illegalLogPath)


    //#############################################
    //   2. ip+port 统计
    //#############################################
    val accessIpPortSql =
      s"""
         |select '$hid' as hid, destip, destport, proctype,
         |       min(acctime) as firsttime,
         |       max(acctime) as activetime,
         |       count(*) as times
         |from ${accessAndIpTable}
         |where iflegal=1
         |group by destip, destport, proctype
         |order by times desc
       """.stripMargin
    val ipPortPath = outputParentPath + "/ipport/hid=" + hid + "/d=" + d + "/h=" +h
    spark.sql(accessIpPortSql).repartition(10).write.mode(SaveMode.Overwrite).format("orc").save(ipPortPath)

    spark.sql(s"select * from $accessAndIpTable ").show()

    //#############################################
    //   3. ip+domain 统计
    //#############################################
    val accessIpDomainSql =
      s"""
         |
       """.stripMargin
  }
}
