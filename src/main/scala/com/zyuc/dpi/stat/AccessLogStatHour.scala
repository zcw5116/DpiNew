package com.zyuc.dpi.stat

import org.apache.spark.sql.SparkSession

/**
  * Created by zhoucw on 上午9:55.
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

    //#############################################
    //   读取访问日志数据
    //#############################################
    val d = hourtime.substring(2, 8)
    val h = hourtime.substring(8, 10)
    val inputPath = inputParentPath + "/" + "hid=" + hid + "/d=" + d + "/h=" + h
    val accessTable = "accessLog"
    val dataDF = spark.read.format("orc").load(inputPath)
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
         |select a.*, case when h.hip is null then 0 else 1 end iflegal
         |from ${accessTable} a left join ${houseIpTable} h
         |on( a.destip = h.hip )
       """.stripMargin
    val accessAndIpTable = "accessAndIp"
    spark.sql(accessAndIpSql).createOrReplaceTempView(accessAndIpTable)

    //#############################################
    //   ip+port 统计
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
    //spark.sql(accessIpPortSql).repartition(10).write.format("orc").save(ipPortPath)

    spark.sql("select domain from accessAndIp where 'v11-dyixigu+acom' rlike '^[0-9a-zA-Z\\-\\\\+]+$' ").show
    //#############################################
    //   ip+domain 统计
    //#############################################
    val accessIpDomainSql =
      s"""
         |
       """.stripMargin
  }
}
