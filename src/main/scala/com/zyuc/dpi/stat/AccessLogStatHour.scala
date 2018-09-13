package com.zyuc.dpi.stat

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoucw on 18-4-24 上午9:55.
  */
object AccessLogStatHour {
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder().enableHiveSupport().appName("AccessLogStatHour").master("local[*]").getOrCreate()
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    //#############################################
    //  参数接收
    //#############################################
    val sc = spark.sparkContext
    val inputParentPath = sc.getConf.get("spark.app.inputParentPath", "/hadoop/accesslog_etl_all/output/data")
    val hid = sc.getConf.get("spark.app.hid", "1019")
    val hourtime = sc.getConf.get("spark.app.hourtime", "2018042408")
    val outputParentPath = sc.getConf.get("spark.app.outputParentPath", "/hadoop/accesslog_stat/hour/out")
    val topDomainInfoFile = sc.getConf.get("spark.app.topDomainInfoFile", "/hadoop/basic/domainInfo.txt")
    val foreignDomainFile = sc.getConf.get("spark.app.foreignDomainFile", "")

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
    //   广播域名基础数据
    //#############################################
    registerTopDomainUDF(spark, topDomainInfoFile)
    registerForeignDomainUDF(spark, foreignDomainFile)

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

    /** 数据流程
      * accesslog:
      *       houseid|sourceip|destip|protocol|sourceport|destport|domain|url|duration|accesstime
      * ip+port+domain:
      *       group accesslog by houseid, destip, destport, domain, protocol
      *       min(accesstime) firsttime,max(accesstime) lasttime,count(1) times
      * ip+port:
      *       group ip+port+domain by houseid, destip, destport, protocol
      *       min(firsttime) firsttime,max(lasttime) lasttime,sum(times) times
      * ip+domain:
      *       group ip+port+domain by houseid, destip, destport, domain
      *       min(firsttime) firsttime,max(lasttime) lasttime,
      *       max(protocal), sum(times) times
      *       domain->topdomain
      * domain:
      *       group ip+domain by houseid, domain
      *       min(firsttime) firsttime,max(lasttime) lasttime,sum(times) times,
      *       max(destip) destip, max(topdomain) topdomain
      */

    //#############################################
    //   1. ip+port+domain 统计
    //#############################################
    val ipPortDomainSql =
    s"""
       |select '$hid' as hid, destip, destport, domain, proctype,
       |        min(acctime) as firsttime,
       |        max(acctime) as activetime,
       |        count(*)     as times
       |from ${accessTable}
       |group by destip, destport, domain, proctype
     """.stripMargin
    val ipPortDomainPath = outputParentPath + "/ipportdomain/hid=" + hid + "/d=" + d + "/h=" + h
    spark.sql(ipPortDomainSql).repartition(1).write.mode(SaveMode.Overwrite)
      .format("orc").save(ipPortDomainPath)

    //#############################################
    //   2. ip+port 统计
    //#############################################
    val ipPortDomainDf = spark.read.format("orc").load(ipPortDomainPath)
    val ipPortDomainTable = "ipPortDomain"
    ipPortDomainDf.createOrReplaceTempView(ipPortDomainTable)
    val accessIpPortSql =
    s"""
       |select '$hid' as hid, destip, destport, proctype,
       |       min(firsttime)  as firsttime,
       |       max(activetime) as activetime,
       |       sum(times)      as times
       |from ${ipPortDomainTable}
       |group by destip, destport, proctype
     """.stripMargin
    val ipPortPath = outputParentPath + "/ipport/hid=" + hid + "/d=" + d + "/h=" + h
    spark.sql(accessIpPortSql).repartition(1).write.mode(SaveMode.Overwrite)
      .format("orc").save(ipPortPath)

    //#############################################
    //   3. ip+domain统计, 去除不规范域名
    //#############################################
    val ipDomainSql =
    s"""
       |select '$hid' as hid, domain, udf_topDomain(domain) topdomain, destip,
       |        firsttime, activetime, times, proctype
       |from
       |(
       |    select domain, destip,
       |           min(firsttime)  as firsttime,
       |           max(activetime) as activetime,
       |           sum(times)      as times,
       |           max(proctype)   as proctype
       |    from ${ipPortDomainTable}
       |    where udf_topDomain(domain) != '-1'
       |    group by domain, destip
       |) t
     """.stripMargin
    val domainIpPath = outputParentPath + "/domainip/hid=" + hid + "/d=" + d + "/h=" + h
    spark.sql(ipDomainSql).repartition(1).write.mode(SaveMode.Overwrite)
      .format("orc").save(domainIpPath)

    //#############################################
    //   4. domain 统计
    //#############################################
    val accessDomainDf = spark.read.format("orc").load(domainIpPath)
    val accessDomainTable = "accessDomain"
    accessDomainDf.createOrReplaceTempView(accessDomainTable)
    // 过滤国外域名
    val addsql = if (foreignDomainFile.length() > 0) {"where udf_foreignDomain(domain) = 0"} else {""}
    val accessDomainSql =
    s"""
       |select '$hid' as hid, domain, udf_foreignDomain(domain) as isforeign,
       |       min(firsttime)  as firsttime,
       |       max(activetime) as activetime,
       |       sum(times)      as times,
       |       max(topdomain)  as topdomain,
       |       max(destip)     as destip
       |from ${accessDomainTable}
       |group by domain
     """.stripMargin
    val domainPath = outputParentPath + "/domain/hid=" + hid + "/d=" + d + "/h=" + h
    val foreignDomainPath = outputParentPath + "/foreigndomain/hid=" + hid + "/d=" + d + "/h=" + h

    val domainDF = spark.sql(accessDomainSql)
    domainDF.filter("isforeign=0").repartition(1).write.mode(SaveMode.Overwrite)
      .format("orc").save(domainPath)
    domainDF.filter("isforeign=1").repartition(1).write.mode(SaveMode.Overwrite)
      .format("csv").options(Map("sep" -> "\\t")).save(foreignDomainPath)
  }

  def registerForeignDomainUDF (spark: SparkSession, foreignDomainFile: String): Unit = {
    if (foreignDomainFile.length() == 0) {
      return
    }
    var dbv_foreDomain: Array[String] = Array()
    val sc = spark.sparkContext
    val foreDomain = sc.textFile(foreignDomainFile).map(x => x.split("-")).filter(_.length == 3).map(x => x(0)).collect()
    val bd_foreDomain = sc.broadcast(foreDomain)
    dbv_foreDomain = bd_foreDomain.value

    spark.udf.register("udf_foreignDomain", (domain: String) => {
      var matchForeign = 0

      val regex = ".*\\.(\\w+)(?:\\:\\d+|)$".r
      if (regex.pattern.matcher(domain).matches()) {
        val regex(suffix) = domain

        if (dbv_foreDomain.contains(suffix)) {
          matchForeign = 1
        }
      }
      matchForeign
    })
  }

  def registerTopDomainUDF (spark: SparkSession, topDomainInfoFile: String): Unit = {
    val sc = spark.sparkContext
    val domainInfo = sc.textFile(topDomainInfoFile).
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
    //  return:  -1:不规范域名  空: 无顶级域名（如ip地址）
    //#############################################
    val bdv_inDomain = bd_inDomain.value
    val bdv_areaDomain = bd_areaDomain.value
    val bdv_countryDomain = bd_countryDomain.value
    val regExpr = "^[\\w\\-:.]+$"
    val pattern = regExpr.r.pattern
    spark.udf.register("udf_topDomain", (domain: String) => {
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
  }
}
