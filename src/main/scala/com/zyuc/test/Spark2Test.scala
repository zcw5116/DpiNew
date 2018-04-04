package com.zyuc.test

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by zhoucw on 上午10:03.
  */
object Spark2Test {
  def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder().appName("test").master("local[3]").getOrCreate()

    val sc = spark.sparkContext
    sc.wholeTextFiles("/tmp/test1").map(x=>(x._1,x._2)).collect().foreach(println)
    val list = new util.ArrayList[Tuple3[String, String, String]]()
    for(i<-1 to 10){
      list.add(("a" + i,"b"+ i, "c"+ i))
    }


   val rdd = sc.parallelize(list.toArray()).asInstanceOf[RDD[Tuple3[String, String, String]]]

   // rdd.map(x=>x._1).collect().foreach(println)

    val str = "/tmp/test1/1005/0x01+0x03a0+000+I-JS-CZQLLWYLS+eversec+038+20180316115502.txt"
    //println(getFileInfo(str))

    def getFileInfo(path:String) :String = {
      var finfo = "-1"
      try{
        val fileName = path.split("\\+")
        finfo = fileName(3) + "+" + fileName(4) + "+" + fileName(5) + fileName(6).substring(0, fileName(6).lastIndexOf("."))
        finfo
      }catch {
        case e:Exception=>{
          finfo
        }
      }
    }
  }
}
