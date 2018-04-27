package com.zyuc.dpi.stat

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zyuc.dpi.etl.AccesslogETL.logger
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by maoheng on 18-4-25 下午2:36.
  */
object AccessLogStatDay {
	val tabDomain = "tmptab_domain"
	val tabTopDomain = "tmptab_topDomain"
//	val tabDomainIp = "tmptab_domainip"
	val tabIllegLog = "tmptab_illeglog"
	val tabIpPort = "tmptab_ipport"
	
	def getSqlMap(table: String): String = {
		table match{
			case `tabDomain` =>
				s"""
				   |select hid, domain,
				   |       min(firsttime) as firsttime, max(activetime) as activetime,
				   |       sum(times) as times, max(destip) as destip,
				   |       max(topdomain) as topdomain
				   |from   $table
				   |group  by hid, domain
				 """.stripMargin
			case `tabTopDomain` =>
				s"""
				   |select hid, topdomain, destip,
				   |       min(firsttime) as firsttime, max(activetime) as activetime,
				   |       sum(times) as times
				   |from   $table
				   |group  by hid, topdomain, destip
				 """.stripMargin
			case `tabIllegLog` =>
				s"""
				   |select distinct hid, domain, destip
				   |from   $table
				 """.stripMargin
			case `tabIpPort` =>
				s"""
				   |select hid, destip, destport, proctype,
				   |       min(firsttime) as firsttime, max(activetime) as activetime,
				   |       sum(times) as times
				   |from   $table
				   |group  by hid, destip, destport, proctype
				 """.stripMargin
		}
	}
	
	def doStat(spark: SparkSession, params: JSONObject): String = {
		val tabLocMap: Map[String, Array[String]] = Map(
			tabDomain -> Array("domain", "domain"),
			tabTopDomain -> Array("domain", "topdomain"),
//			tabDomainIp -> "domainip",
			tabIllegLog -> Array("illegLog", "illegLog"),
			tabIpPort -> Array("ipport", "ipport")
		)
		
		val appName = params.get("appName")
		val inputParentPath = params.get("inputParentPath")
		val outputParentPath = params.get("outputParentPath")
		val hid = params.get("hid")
		val statDate = params.get("statDate")

		tabLocMap.keys.foreach(tabName => {
			val inputLocation = inputParentPath + "/" + tabLocMap.get(tabName).map(_(0)).getOrElse("") + "/hid=" + hid + "/d=" + statDate.toString.substring(2) + "/*/*.orc"
			val outputLocation = outputParentPath + "/" + tabLocMap.get(tabName).map(_(1)).getOrElse("")+ "/hid=" + hid + "/d=" + statDate.toString.substring(2)
			
			val df = spark.read.format("orc").load(inputLocation)
			
			if (df != null) {
//				df.printSchema()
				df.createOrReplaceTempView(tabName)
				val sql = getSqlMap(tabName)
				
				spark.sql(sql).repartition(10).write.mode(SaveMode.Overwrite)
					.format("csv").options(Map("header" -> "true"))
					.save(outputLocation)
			}
			else {
				logger.info(s"location{$inputLocation} is empty")
			}
		})
		
		logger.info(s"[$appName] success")
		null
	}
	
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().enableHiveSupport()
	    	.appName("AccessLogStat_test").master("local[2]").getOrCreate()
		
		val sc = spark.sparkContext
		
		val appName = sc.getConf.get("appName", "AccessLogStat_test")
		val inputParentPath = sc.getConf.get("spark.app.inputParentPath", "/hadoop/accesslog_stat/hour/out")
		val outputParentPath = sc.getConf.get("spark.app.outputParentPath", "/hadoop/accesslog_stat/day/out")
		val hid = sc.getConf.get("spark.app.hid", "1019")
		val statDate = sc.getConf.get("spark.app.statDate", "20180425")
		
		val paramString: String =
			s"""
			   |{
			   | "appName"          : "$appName",
			   | "inputParentPath"  : "$inputParentPath",
			   | "outputParentPath" : "$outputParentPath",
			   | "hid"              : "$hid",
			   | "statDate"         : "$statDate"
			   |}
			 """.stripMargin
		
		val params = JSON.parseObject(paramString)
		val rst = doStat(spark, params)
	}
}
