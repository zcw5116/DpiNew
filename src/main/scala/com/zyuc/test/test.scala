package com.zyuc.test

import com.zyuc.dpi.utils.{FileUtils, JsonValueNotNullException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, sum}
import org.apache.spark.sql.hive.HiveContext


/**
  * Created by zhoucw on 18-4-24 下午7:13.
  */
object test {



    def mvFiles(fileSystem: FileSystem, fromDir: String, toDir: String): Unit = {
      try {
        fileSystem.globStatus(new Path(fromDir + "/*")).foreach(x => {
          val hidDir = x.getPath.getName
          if(!fileSystem.exists(new Path(toDir + "/" + hidDir ))){
            fileSystem.mkdirs(new Path(toDir + "/" + hidDir ))
          }

          fileSystem.globStatus(new Path(s"${fromDir}/${hidDir}/*")).foreach(f => {
            val fname = f.getPath.getName
            println("fname:" +fname)
            val newPath = new Path(toDir + "/" + hidDir + "/" + fname)

            println(s"${fromDir}/${hidDir},${toDir}/${hidDir}, result:" + fileSystem.rename(f.getPath, newPath))

          })
        })
        val fromSize = fileSystem.getContentSummary(new Path(fromDir)).getLength
        if(fromSize==0){
          fileSystem.delete(new Path(fromDir), true)
        }

      } catch {
        case e: Exception => {
          throw new Exception(s"move files from ${fromDir} to ${toDir} failed." + e.getMessage)
        }
      }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().appName("AccessLogStatHour").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    // /hadoop/test1/201807251205
    mvFiles(fileSystem, "/hadoop/test1/201807251205", "/hadoop/test1/201807251205_done")
  }
}
