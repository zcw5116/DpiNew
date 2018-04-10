package com.zyuc.test

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by zhoucw on 上午10:03.
  */
object Spark2Test {
  def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder().enableHiveSupport().appName("test").master("local[3]").getOrCreate()
    val sc = spark.sparkContext

    val textRdd = sc.textFile("/tmp/test.txt")
    val rdd = textRdd.map(x=>x.split(",")).map(row=>(row(0), row(1))).groupByKey(12).
      map(x=>{
          (x._1, x._2.iterator.length)
        })

    rdd.count()
  }
}
