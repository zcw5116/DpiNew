package com.zyuc.test


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by zhoucw on 18-4-24 上午10:03.
  */
object Spark2Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().appName("test").master("local[3]").getOrCreate()
    val sc = spark.sparkContext



    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val isb = fileSystem.exists(new Path("/tmp/2018_*"))
    println("isb:" + isb)

  }
}
