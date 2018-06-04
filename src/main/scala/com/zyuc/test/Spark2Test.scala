package com.zyuc.test

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by zhoucw on 18-4-24 上午10:03.
  */
object Spark2Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().appName("test").master("local[3]").getOrCreate()
    val sc = spark.sparkContext

    val struct = StructType(Array(
      StructField("a", StringType),
      StructField("b", StringType),
      StructField("c", StringType),
      StructField("d", StringType),
      StructField("e", IntegerType),

      StructField("f", StringType),
      StructField("g", StringType)
    ))

    val rowRDD = sc.textFile("/hadoop/accesslog_stat/day/out/domain/hid=246/d=180522").map(x=>x.split("\\t")).
      map(x=>Row(x(0), x(1), x(2), x(3),x(4), x(5), x(6)))

    val df = spark.createDataFrame(rowRDD, struct)
    df.printSchema()

  }
}
