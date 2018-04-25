package com.zyuc.dpi.etl.realtime

import com.zyuc.dpi.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import scala.collection.mutable

/**
  * Created by liuzk on 18-4-19.
  */
object CoalesceETL {
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder().enableHiveSupport().appName("name_20180418").master("local[3]").getOrCreate()
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val appName = sc.getConf.get("spark.app.name")
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val inputPath = sc.getConf.get("spark.app.inputPath")
    val childPath = sc.getConf.get("spark.app.childPath")
    val mergePath = sc.getConf.get("spark.app.mergePath")
    val partitionSize = sc.getConf.get("spark.app.partitionSize","50").toInt
    val fileSizeLess = sc.getConf.get("spark.app.fileSizeLess","50")
    val fileSize = (fileSizeLess.toInt)*1024*1024
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    mergeFiles(sqlContext, fileSystem, dataTime, inputPath, childPath, mergePath,partitionSize,fileSize)

  }
  def mergeFiles(parentContext:SQLContext, fileSystem:FileSystem, batchTime:String, inputPath:String,
                 childPath:String, mergePath:String,partitionSize:Int,fileSize:Int): String ={
    val sqlContext = parentContext.newSession()
    val srcDataPath = inputPath + childPath
    val litterFilePath = inputPath + "/litterFile" + childPath

    val mergeSrcPath = mergePath + "/" + batchTime + "/src" + childPath
    val mergeDataPath = mergePath + "/" + batchTime + "/data" + childPath

    var mergeInfo = "merge success"
    var num = 0
    val filesizesmallSet = new mutable.HashSet[String]()

    try{
      fileSystem.globStatus(new Path(srcDataPath + "/*")).foreach(x=>{
        //取文件大小小于50M的
        if(x.getLen < fileSize){
          filesizesmallSet += (x.getPath.toString)
        }
      })
      filesizesmallSet.foreach(f => {
        val fileName = f.substring(f.lastIndexOf("/") + 1)
        val fromPath = new Path(f)

        if (fileName != "_SUCCESS") {
          var destLocation = f.replace(srcDataPath, litterFilePath)
          val fileSuffix = if (fileName.contains(".")) fileName.substring(fileName.lastIndexOf(".")) else ""
          val newFileName = batchTime + "_" + num + fileSuffix

          destLocation = destLocation.substring(0, destLocation.lastIndexOf("/") + 1) + newFileName
          num = num + 1
          val destPath = new Path(destLocation)

          if (!fileSystem.exists(destPath.getParent)) {
            fileSystem.mkdirs(destPath.getParent)
          }
          fileSystem.rename(fromPath, destPath)
        }
      })


      val partitionNum = FileUtils.computePartitionNum(fileSystem, litterFilePath, partitionSize)
      // 将需要合并的文件mv到临时目录
      FileUtils.moveFiles(fileSystem, batchTime, litterFilePath, mergeSrcPath, true)
      val srcDF = sqlContext.read.format("orc").load(mergeSrcPath + "/")
      // 将合并目录的src子目录下的文件合并后保存到合并目录的data子目录下
      srcDF.coalesce(partitionNum).write.format("orc").mode(SaveMode.Overwrite).save(mergeDataPath)
      // 将合并目录的data目录下的文件移动到原目录
      FileUtils.moveFiles(fileSystem, batchTime, mergeDataPath, srcDataPath, false)

      // 删除 合并目录src的子目录
      fileSystem.delete(new Path(mergePath + "/" + batchTime), true)

    }catch {
      case e:Exception => {
        e.printStackTrace()
        mergeInfo = "merge failed"
      }
    }

    mergeInfo
  }

}