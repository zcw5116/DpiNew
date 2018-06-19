package com.zyuc.dpi.query

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat

import org.apache.spark.sql.SaveMode

import scala.collection.mutable

object IDCAccessLogMR {
  def main(args: Array[String]) {
    try{
      if (args.length != 3){
        System.exit(2)
      }else{
        println("QueryParams: " + args(0) + ", Input: " + args(1) + ", Output: " + args(2))
      }

      val spark = SparkSession.builder().appName("IDCAccessLogMR").enableHiveSupport().getOrCreate()
      val sc = spark.sparkContext
      val fileSystem = FileSystem.get(sc.hadoopConfiguration)

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val input:String = args(1)

      val params: Array[String] = args(0).split(",")
      val house_ID:String = params(0).substring(params(0).indexOf("=")+1)
      val startTime:String = params(1).substring(params(1).indexOf("=")+1)
      val endTime:String = params(2).substring(params(2).indexOf("=")+1)
      val startDate:String = sdf.format(startTime.toLong*1000)
      val endDate:String = sdf.format(endTime.toLong*1000)
      val BeginSourceIP:String =params(3).substring(params(3).indexOf("=") + 1)
      val EndSourceIP:String =params(4).substring(params(4).indexOf("=") + 1)
      val BeginDestinationIP:String =params(5).substring(params(5).indexOf("=") + 1)
      val EndDestinationIP:String =params(6).substring(params(6).indexOf("=") + 1)
      val SourcePort:String =params(7).substring(params(7).indexOf("=") + 1)
      val DestinationPort:String =params(8).substring(params(8).indexOf("=") + 1)
      val Domain_Name:String =params(9).substring(params(9).indexOf("=") + 1)
      val URL:String =params(10).substring(params(10).indexOf("=") + 1)
      val ProtocolType:String =params(11).substring(params(11).indexOf("=") + 1)
      val Duration:String =params(12).substring(params(12).indexOf("=") + 1)
      val timepara:String = params(13).substring(params(13).indexOf("=") + 1)
      val basePath = params(14).substring(params(14).indexOf("=") + 1) // /hadoop/accesslog_etl/output/data
      val partitionNum = params(14).substring(params(14).indexOf("=") + 1)
      val ifCalCnt = 1

 /*     val files = dealFileByTime(input, house_ID, startTime, endTime,timepara.toInt,fileSystem)
      println("files:"+files)
      if (files != null) {
        println("files.length:"+files.length)
      }
*/
      var start:Long = startTime.toLong * 1000
      val end:Long = endTime.toLong * 1000
      // 根据开始时间/结束时间返回分区路径的数组
      val pathArr = getLoadPath(basePath + "/hid=" + house_ID, fileSystem, start, end)


      if(pathArr != null && pathArr.length>0){
        spark.udf.register("udf_ip2long", (strIp: String) => {
          var result:Long = 0
          try{
            if (strIp != null && !"".equals(strIp.trim())) {
              val position1 = strIp.indexOf(".")
              val position2 = strIp.indexOf(".", position1 + 1)
              val position3 = strIp.indexOf(".", position2 + 1)

              if (position1 > -1 && position2 > -1 && position3 > -1){
                val ip0 = strIp.substring(0, position1).toLong
                val ip1 = strIp.substring(position1+1, position2).toLong
                val ip2 = strIp.substring(position2+1, position3) .toLong
                val ip3 = strIp.substring(position3+1).toLong
                result = (ip0 << 24) + (ip1 << 16) + (ip2 <<  8 ) + ip3
              }
            }
          }catch {
            case e:Exception=>{e.printStackTrace()}
          }
          result
        })

        var strfilter:String = "acctime>='"+startDate+"' and acctime<='"+endDate+"'"
        if (BeginSourceIP != null && !"".equals(BeginSourceIP)){
          strfilter = strfilter + " and udf_ip2long(srcip) >= '"+ipToLong(BeginSourceIP)+"'"
        }
        if (EndSourceIP != null && !"".equals(EndSourceIP)){
          strfilter = strfilter + " and udf_ip2long(srcip) <= '"+ipToLong(EndSourceIP)+"'"
        }
        if (BeginDestinationIP != null && !"".equals(BeginDestinationIP)){
          strfilter = strfilter + " and udf_ip2long(destip) >= '"+ipToLong(BeginDestinationIP)+"'"
        }
        if (EndDestinationIP != null && !"".equals(EndDestinationIP)){
          strfilter = strfilter + " and udf_ip2long(destip) <= '"+ipToLong(EndDestinationIP)+"'"
        }
        if (SourcePort != null && !"".equals(SourcePort)){
          strfilter = strfilter + " and srcport = '"+SourcePort+"'"
        }
        if (DestinationPort != null && !"".equals(DestinationPort)){
          strfilter = strfilter + " and destport = '"+DestinationPort+"'"
        }
        if (URL != null && !"".equals(URL)){
          strfilter = strfilter + " and url = '"+URL+"'"
        }
        if (Domain_Name != null && !"".equals(Domain_Name)){
          strfilter = strfilter + " and domain ='"+Domain_Name+"'"
        }
        if (ProtocolType != null && !"".equals(ProtocolType)){
          strfilter = strfilter + " and proctype ='"+ProtocolType+"'"
        }
       // println("strfilter:"+strfilter)
       // println("files.length:"+files.length)

     /*   var df=spark.read.format("orc").load(files(0))
        if (files.length >1) {
          for (i <- 0 until files.length) {
            val tmpDF = spark.read.format("orc").load(files(i))
            df = df.union(tmpDF)
          }
        }
*/

        val df = spark.read.format("orc").options(Map("basePath"->basePath)).load(pathArr:_*)

        val resultDF = df.filter(strfilter)

        // 如果是管据下发指令， 不统计count
        val count:Long = resultDF.count()
        val outfile:String = args(2) + count.toString()

        resultDF.repartition(partitionNum.toInt).write.format("csv").
          mode(SaveMode.Overwrite).options(Map("sep" -> ",")).save(outfile)

        var cnt = 0l
        if (ifCalCnt == "1") {
          cnt = spark.read.format("csv").options(Map("sep" -> ",")).load(outfile).count()
        }




        System.exit(0)
      }else{
        System.exit(1)
      }
    }catch{
      case e: Exception => println("exception:"+e.getMessage); System.exit(-1)
    }

  }

  /**
    * 根据开始时间和结束时间生成hdfs目录， 并将目录保存到数组中
    * 如果分区目录不存在, 剔除
    * @param inputPath
    * @param fileSystem
    * @param begin      开始的unixtime
    * @param end        结束的unixtime
    * @return
    */
  def getLoadPath(inputPath: String, fileSystem:FileSystem, begin: Long, end: Long): Array[String] = {

    val sdf = new SimpleDateFormat("yyyyMMddHHmmSS")
   // val begin = sdf.parse(beginTime).getTime
   // val end = sdf.parse(endTime).getTime

    // 使用set保存分区目录
    val partitionSet = new mutable.HashSet[String]()

    /**
      *  根据时间生成分区目录
      *  example:
      *          参数： 201806130821的unixtime, 返回结果根据5分钟向下取整数
      *          返回分区目录： /hadoop/accesslog_etl/output/data/hid=1005/d=180613/h=08/m5=20
      * @param time
      * @return
      */
    def getPathByTime(time:Long):String = {
      val yyyyMMddHHmm = sdf.format(time)
      val d = yyyyMMddHHmm.substring(2, 8)
      val h = yyyyMMddHHmm.substring(8, 10)
      val m5 = yyyyMMddHHmm.substring(10, 11) + (yyyyMMddHHmm.substring(11, 12).toInt / 5) * 5
      inputPath + "/d=" + d + "/h=" + h + "/m5=" + m5
    }


    // 遍历开始时间和结束时间， 将分区目录保存到集合中
    // 注意： 如果目录不存在，则不能加入到集合中， 否则使用spark的外部数据源读取报错终止
    var time: Long = begin
    while(time < end ){
      val path = getPathByTime(time)
      if(fileSystem.exists(new Path(path))){
        partitionSet .+=(path)
      }
      time = time + 5 * 60 * 1000
    }
    // 根据结束时间生成分区目录，保存到set集合中， 即使有重复元素也不要紧，set集合元素都是不重复的
    // 注意： 如果目录不存在，则不能加入到集合中， 否则使用spark的外部数据源读取报错终止
    val path = getPathByTime(end)
    if(fileSystem.exists(new Path(path))){
      partitionSet .+=(path)
    }

    // 将分区目录的集合转换成数组
    partitionSet.toArray
  }




  def dealFileByTime(input:String,house_ID:String,startTimeS:String,endTimeS:String,timepara:Int,fileSystem:FileSystem):ArrayBuffer[String] = {
    val filePath = ArrayBuffer[String]()

    //根据参数获取结束前的时间段
    var startTime:Long = startTimeS.toLong * 1000
    val endTime:Long = endTimeS.toLong * 1000
    val times = (endTime - startTime)/1000/60/60
    val timemms = (endTime - startTime)%(1000*60*60)

    if (times > timepara) {
      startTime = endTime - timepara*60*60*1000
    } else {
      if (times == timepara && timemms > 0) {
        startTime = endTime - timepara*60*60*1000
      }
    }

    val sdf_d = new SimpleDateFormat("yyMMdd")
    val sdf_h = new SimpleDateFormat("HHmm")

    val s_date:String = sdf_d.format(startTime)
    val s_hour:String = sdf_h.format(startTime)
    val e_date:String = sdf_d.format(endTime)
    val e_hour:String = sdf_h.format(endTime)
    //判断跨越几天
    val day:Long = (endTime - startTime)/1000/60/60/24

    val timelist = ArrayBuffer[String]()
    val myArray = Array("00", "05", "10","15","20","25","30","35","40","45","45","50","55")
    for (a <- 0 until 24) {
      var hour:String = a.toString()
      if(a<10){
        hour = "0"+ hour
      }
      for(b <- 0 until myArray.length){
        timelist += hour + myArray(b)
      }
    }

    if (day == 0){
      for (i <- 0 until timelist.length){
        var filepathstr:String = ""
        if(s_hour.toInt >= timelist(i).toInt && s_hour.toInt < timelist(i+1).toInt){
          val hourstr = timelist(i).substring(0, 2)
          val minutestr = timelist(i).substring(2)

          filepathstr = input + "/hid=" + house_ID + "/d=" + s_date + "/h=" + hourstr + "/m5=" + minutestr
        }
        if (s_hour.toInt < timelist(i).toInt && e_hour.toInt >= timelist(i).toInt){
          val hourstr = timelist(i).substring(0, 2)
          val minutestr = timelist(i).substring(2)

          filepathstr = input + "/hid=" + house_ID + "/d=" + s_date + "/h=" + hourstr + "/m5=" + minutestr
        }
        if(filepathstr != null && !"".equals(filepathstr)){
          if(fileSystem.exists(new Path(filepathstr))){
            filePath += filepathstr
          }
        }
      }

    }else{
      //第一天
      val e_hour_t:String = "2359"
      for (i <- 0 until timelist.length) {
        var filepathstr:String = ""
        if(s_hour.toInt >= timelist(i).toInt && s_hour.toInt < timelist(i+1).toInt){
          val hourstr = timelist(i).substring(0, 2)
          val minutestr = timelist(i).substring(2)
          filepathstr = input + "/hid=" + house_ID + "/d=" + s_date + "/h=" + hourstr + "/m5=" + minutestr
        }
        if (s_hour.toInt < timelist(i).toInt && e_hour_t.toInt >= timelist(i).toInt){
          val hourstr = timelist(i).substring(0, 2)
          val minutestr = timelist(i).substring(2)
          filepathstr = input + "/hid=" + house_ID + "/d=" + s_date + "/h=" + hourstr + "/m5=" + minutestr
        }
        if(filepathstr != null && !"".equals(filepathstr)){
          if(fileSystem.exists(new Path(filepathstr))){
            filePath += filepathstr
          }
        }
      }
      //中间天数
      if (day > 1) {
        val days = day.toInt
        for (m <- 1 until days) {
          var filepathstr:String = ""
          val startDateStr = sdf_d.format(startTime+m*1000*60*60*24)
          filepathstr = input + "/hid=" + house_ID + "/d=" + startDateStr
          if(filepathstr != null && !"".equals(filepathstr)){
            if(fileSystem.exists(new Path(filepathstr))){
              filePath += filepathstr
            }
          }
        }
      }

      //最后一天
      val s_hour_t = "0000"
      for (m <- 0 until timelist.length){
        if (s_hour_t.toInt <= timelist(m).toInt && e_hour.toInt >= timelist(m).toInt){
          val hourstr = timelist(m).substring(0, 2)
          val minutestr = timelist(m).substring(2)
          val filepathstr = input + "/hid=" + house_ID + "/d=" + e_date + "/h=" + hourstr + "/m5=" + minutestr
          if(fileSystem.exists(new Path(filepathstr))){
            filePath += filepathstr
          }
        }
      }
    }

    filePath
  }

  def ipToLong(strIp: String): Long = {
    var result:Long = 0
    try {
      if (strIp != null && !"".equals(strIp.trim())){

        val position1 = strIp.indexOf(".");
        val position2 = strIp.indexOf(".", position1 + 1);
        val position3 = strIp.indexOf(".", position2 + 1);

        if (position1 > -1 && position2 > -1 && position3 > -1){
          val ip0 = strIp.substring(0, position1).toLong
          val ip1 = strIp.substring(position1+1, position2).toLong
          val ip2 = strIp.substring(position2+1, position3) .toLong
          val ip3 = strIp.substring(position3+1).toLong
          result = (ip0 << 24) + (ip1 << 16) + (ip2 <<  8 ) + ip3
        }

      }
    } catch {
      case ex: Exception =>{
        println("exception e:"+ex)
      }
    }

    result
  }

}