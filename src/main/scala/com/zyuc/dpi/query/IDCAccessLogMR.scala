package com.zyuc.dpi.query

/**
  * Created by zhoucw on 18-5-22 下午6:03.
  */

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import org.apache.spark.sql.SaveMode






object IDCAccessLogMR {
  def main(args: Array[String]) {
    try{
      if (args.length != 3){
        System.exit(2)
      }else{
        println("QueryParams: " + args(0) + ", Input: " + args(1) + ", Output: " + args(2))
      }

      val input:String = args(1)
      val outfile:String = args(2) + "/result"

      val params: Array[String] = args(0).split(",")
      val house_ID:String = params(0).substring(params(0).indexOf("=")+1)
      val startTime:String = params(1).substring(params(1).indexOf("=")+1)
      val endTime:String = params(2).substring(params(2).indexOf("=")+1)
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

      val files = dealFileByTime(input, house_ID, startTime, endTime,timepara.toInt)
      println("files:"+files)
      println((files != null && files.length>0))
      if (files != null) {
        println("files.length:"+files.length)
      }

      if(files != null && files.length>0){
        val spark = SparkSession.builder().appName("IDCAccessLogMR").enableHiveSupport().getOrCreate()
        val sc = spark.sparkContext

        val df=spark.read.format("orc").load(files(0))

        var strfilter:String = "House_ID = '"+house_ID+"'"
        if (startTime != null && !"".equals(startTime)){
          strfilter = strfilter + " and  acctime>='"+startTime+"'"
        }
        if (endTime != null && !"".equals(endTime)){
          strfilter = strfilter + " and acctime<='"+endTime+"'"
        }
        if (BeginSourceIP != null && !"".equals(BeginSourceIP)){
          strfilter = strfilter + " and srcip >= '"+BeginSourceIP+"'"
        }
        if (EndSourceIP != null && !"".equals(EndSourceIP)){
          strfilter = strfilter + " and srcip <= '"+EndSourceIP+"'"
        }
        if (BeginDestinationIP != null && !"".equals(BeginDestinationIP)){
          strfilter = strfilter + " and destip >= '"+BeginDestinationIP+"'"
        }
        if (EndDestinationIP != null && !"".equals(EndDestinationIP)){
          strfilter = strfilter + " and destip <= '"+EndDestinationIP+"'"
        }
        if (SourcePort != null && !"".equals(SourcePort)){
          strfilter = strfilter + " and srcport = '"+SourcePort+"'"
        }
        if (DestinationPort != null && !"".equals(DestinationPort)){
          strfilter = strfilter + " and destport = '"+DestinationPort+"'"
        }
        if (URL != null && !"".equals(URL)){
          strfilter = strfilter + " and url like '%"+URL+"%'"
        }
        if (Domain_Name != null && !"".equals(Domain_Name)){
          strfilter = strfilter + " and domain ='"+Domain_Name+"'"
        }
        if (ProtocolType != null && !"".equals(ProtocolType)){
          strfilter = strfilter + " and proctype ='"+ProtocolType+"'"
        }
        println("strfilter:"+strfilter)
        println("files.length:"+files.length)

        val resultDF = df.filter(strfilter)

        resultDF.write.format("csv").mode(SaveMode.Overwrite).options(Map("sep" -> ",")).save(outfile)

        sc.stop()

        System.exit(0)
      }else{
        println("========files:"+files)
        if (files != null) {
          println("=====files.length:"+files.length)
        }
        System.exit(1)
      }
    }catch{
      case e: Exception => println("exception:"+e.getMessage); System.exit(-1)
    }

  }

  def dealFileByTime(input:String,house_ID:String,startTimeOld:String,endTime:String,timepara:Int):ArrayBuffer[String] = {
    val filePath = ArrayBuffer[String]()

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val sdf_d = new SimpleDateFormat("yyMMdd")
    val sdf_h = new SimpleDateFormat("HHmm")

    val sdfall = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startall:Long = sdfall.parse(sdfall.format(startTimeOld.toLong*1000)).getTime()
    val endall = sdfall.parse(sdfall.format(endTime.toLong*1000)).getTime()
    val times = (endall - startall)/1000/60/60
    val timemms = (endall - startall)%(1000*60*60)
    var startTime:String = startTimeOld
    if (times > timepara) {
      val startnew:Long = (endall - timepara*60*60*1000)/1000;
      startTime = startnew.toString()
    } else {
      if (times == timepara && timemms > 0) {
        val startnew:Long = (endall - timepara*60*60*1000)/1000;
        startTime = startnew.toString()
      }
    }

    val  s_date:String = sdf_d.format(startTime.toLong*1000)
    val s_hour:String = sdf_h.format(startTime.toLong*1000)
    val e_date:String = sdf_d.format(endTime.toLong*1000)
    val e_hour:String = sdf_h.format(endTime.toLong*1000)

    val start:Long = sdf.parse(sdf.format(startTime.toLong*1000)).getTime()
    val end:Long = sdf.parse(sdf.format(endTime.toLong*1000)).getTime()
    val day:Long = (end - start)/1000/60/60/24

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

    val conf:Configuration = new Configuration()
    val filesys:FileSystem = FileSystem.get(conf)

    if (day == 0){
      var checkfile= false
      var dataStr = ""
      for (i <- 0 until timelist.length){
        var filepathstr:String = ""
        if(s_hour.toInt >= timelist(i).toInt && s_hour.toInt <= timelist(i+1).toInt){
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
          if(filesys.exists(new Path(filepathstr))){
            filePath += filepathstr
          }
        }
      }

    }else{

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