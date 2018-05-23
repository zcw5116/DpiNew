package com.zyuc.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, sum}
import org.apache.spark.sql.hive.HiveContext


/**
  * Created by zhoucw on 18-4-24 下午7:13.
  */
object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val dataSet = List((1, 2.0), (1, 3.0), (1, 1.0), (1, -2.0), (1, -1.0))

    import spark.implicits._
    var df = sc.parallelize(dataSet).map(x => (x._1, x._2)).toDF("x", "AmtPaid")

    val wd = Window.partitionBy("x").rowsBetween(Long.MinValue, 0)

    df = df.withColumn("AmtPaidCumSum", sum($"AmtPaid").over(wd))

    df = df.withColumn("AmtPaidCumSumMax", max($"AmtPaidCumSum").over(wd))
    df.show()
  }
}
