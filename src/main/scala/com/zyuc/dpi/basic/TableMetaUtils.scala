package com.zyuc.dpi.basic

import java.sql.{Connection, PreparedStatement}
import java.util
import com.zyuc.dpi.utils.DbUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.Array._
import scala.collection.mutable


/**
  * Created by zhoucw on 下午3:50.
  * dpihouseinfo
  */
object TableMetaUtils {

  val logger = Logger.getLogger("com.zyuc.dpi")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().master("local[2]").appName("test").getOrCreate()
    val sc = spark.sparkContext
    val appConf = sc.getConf.get("spark.app.appConf", "")
    val dataPath = sc.getConf.get("spark.app.dataPath", "/tmp/abc/")
    val houseDataAction = sc.getConf.get("spark.app.houseDataAction", "read")  // read  write
    val dayid = sc.getConf.get("spark.app.dayid", "20180322")
    val accessTable = sc.getConf.get("spark.app.accessTable", "dpi_log_access_m5")
    val d = dayid.substring(2, 8)

    val partitions = generateM5Partitions(spark, dataPath, "20180326")
    val rdd = sc.makeRDD(partitions.toSeq)
  }

  /**
    * 按照机房/日/小时/5分钟 生成分区集合
    * @param spark
    * @param dataPath
    * @param dayid
    */
  def generateM5Partitions(spark:SparkSession, dataPath:String, dayid:String):mutable.HashSet[String] = {
    val d = dayid.substring(2, 8)
    val partitionSet = new mutable.HashSet[String]()
    val houseidArr = spark.sparkContext.textFile(dataPath).map(x=>x.split(",")(0)).collect()
    houseidArr.foreach(hid=>{
      range(0,24).foreach(hour=>{
        val h = java.lang.String.format("%02d",java.lang.Integer.valueOf(hour))
        range(0, 59, 5).foreach(minute=>{
          val m5 = java.lang.String.format("%02d",java.lang.Integer.valueOf(minute))
          partitionSet .+= ("/hid=" + hid + "/d=" + d + "/h=" + h + "/m5=" + m5)
        })
      })
    })
    partitionSet
  }

  /**
    *  刷新分区
    * @param spark
    * @param table
    * @param partitionSets
    */
  def refreshPartition(spark:SparkSession, table:String, partitionSets:mutable.HashSet[String]) = {
    partitionSets.foreach(p=>{
      var hid = ""
      var d = ""
      var h = ""
      var m5 = ""
      p.split("/").map(x=>{
        if(x.startsWith("hid=")){
          hid = x.substring(4)
        } else if(x.startsWith("d=")){
          d = d.substring(2)
        } else if(x.startsWith("h=")){
          h = h.substring(2)
        } else if(x.startsWith("m5=")){
          m5 = m5.substring(3)
        }
        null
      })
      var sql = ""
      if(hid.nonEmpty && d.nonEmpty && h.nonEmpty && m5.nonEmpty){
        val sql = s"alter table ${table} add IF NOT EXISTS partition(hid='$hid', d='$d', h='$h',m5='$m5')"
      }
      else if(hid.nonEmpty && d.nonEmpty && h.nonEmpty){
        val sql = s"alter table ${table} add IF NOT EXISTS partition(hid='$hid', d='$d', h='$h')"
      }
      if(sql.nonEmpty){
        spark.sql(sql)
      }
    })
  }

  def getDpiHouseInfoFromOracle() = {
    var dbConnection:Connection = null
    var preparedStatement:PreparedStatement = null
    val sql = "select houseid, housename from dpihouseinfo"
    val result = new util.ArrayList[Tuple2[String, String]]()

    try{
      dbConnection = DbUtils.getDBConnection()
      preparedStatement = dbConnection.prepareStatement(sql)
      // execute select SQL stetement
      val rs = preparedStatement.executeQuery()

      while(rs.next()){
        result.add((rs.getString(1), rs.getString(2)))
      }
    }catch {
      case e:Exception => {
        e.printStackTrace()
      }
    }finally {
      if(preparedStatement != null){
        preparedStatement.close()
      }
      if(dbConnection != null){
        dbConnection.close();
      }
    }
    result
  }

  /**
    * 从oracle抽取机房数据保存到hdfs
    * @param spark
    */
  def getHouseInfo2HDFS(spark:SparkSession, dataPath:String) = {
    val result = getDpiHouseInfoFromOracle
    val rdd = spark.sparkContext.parallelize(result.toArray()).asInstanceOf[RDD[Tuple2[String, String]]]
    import spark.implicits._
    val df = rdd.toDF("houseid", "housename")
    df.selectExpr("concat_ws(',', houseid, housename)").coalesce(1)
      .write.format("text").mode(SaveMode.Overwrite).save(dataPath)
  }


}
