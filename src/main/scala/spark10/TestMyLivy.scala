package spark10

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoucw on 上午3:56.
  */
object TestMyLivy {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext

    val list = List("1", "2", "a", "b", "c", "d", "e", "f", "g", "h")

    val rdd = sc.makeRDD(list)

    rdd.collect().foreach(println)
  }
}
