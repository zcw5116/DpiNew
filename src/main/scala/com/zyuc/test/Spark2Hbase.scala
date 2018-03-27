package com.zyuc.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  * Created by zhoucw on 上午10:21.
  */
object Spark2Hbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val list = List((1,"123", "abc"), (2, "456", "def"), (3, "789", "ghi"))
    val rdd = sc.makeRDD(list)

    val hTableName = "test111"
    var jobConf = new JobConf(HBaseConfiguration.create)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hTableName)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val hbaseRDD = rdd.map(x=>{
      val rowkey = x._1
      val put = new Put(Bytes.toBytes(rowkey))

      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ifParseNormal"), Bytes.toBytes(x._2))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("houseid"), Bytes.toBytes(x._3))
      (new ImmutableBytesWritable , put)
    })
    hbaseRDD.saveAsHadoopDataset(jobConf)
  }
}
