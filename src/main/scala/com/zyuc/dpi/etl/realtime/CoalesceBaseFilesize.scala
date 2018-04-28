package com.zyuc.dpi.etl.realtime

import com.zyuc.dpi.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import scala.collection.mutable

/**
  * Created by liuzk on 18-4-19.
  */
object CoalesceBaseFilesize {
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
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    mergeFiles(sqlContext, fileSystem, dataTime, inputPath, childPath, mergePath,partitionSize)

  }
  def mergeFiles(parentContext:SQLContext, fileSystem:FileSystem, batchTime:String, inputPath:String,
                 childPath:String, mergePath:String,partitionSize:Int): String ={
    val sqlContext = parentContext.newSession()
    val srcDataPath = inputPath + childPath

    val fileSizeLess64Path = inputPath + "/fileSizeLess64" + childPath
    val fileSizeLess42Path = inputPath + "/fileSizeLess42" + childPath
    val fileSizeLess25Path = inputPath + "/fileSizeLess25" + childPath
    val mergeSrc64Path = mergePath + "/" + batchTime + "/src64" + childPath
    val mergeSrc42Path = mergePath + "/" + batchTime + "/src42" + childPath
    val mergeSrc25Path = mergePath + "/" + batchTime + "/src25" + childPath
    val mergeData64Path = mergePath + "/" + batchTime + "/data64" + childPath
    val mergeData42Path = mergePath + "/" + batchTime + "/data42" + childPath
    val mergeData25Path = mergePath + "/" + batchTime + "/data25" + childPath

    var mergeInfo = "merge success"
    var num = 0
    var sum64To42 = 0
    var sum42To25 = 0
    var sumLess25 = 0
    var coalNum = 1

    val fileSizesLess64Set = new mutable.HashSet[String]()
    val fileSizesLess42Set = new mutable.HashSet[String]()
    val fileSizesLess25Set = new mutable.HashSet[String]()

    try{
      fileSystem.globStatus(new Path(srcDataPath + "/*")).foreach(x=>{
        //按文件大小存入相应set
        if(x.getLen > 42*1024*1024 && x.getLen <= 64*1024*1024){
          sum64To42 += 1
          fileSizesLess64Set += x.getPath.toString
        }else if(x.getLen > 25*1024*1024 && x.getLen <= 42*1024*1024){
          sum42To25 += 1
          fileSizesLess42Set += x.getPath.toString
        }else if(x.getLen <= 25*1024*1024){
          sumLess25 +=1
          fileSizesLess25Set += x.getPath.toString
        }
      })
      println("64-" + sum64To42 + "-42-" + sum42To25 + "-25-" + sumLess25)
      //文件大小64~42
      if(sum64To42 > 1 && sum64To42 % 2==0){
        coalNum = sum64To42/2
        setToDir(fileSizesLess64Set,fileSizeLess64Path,mergeSrc64Path,mergeData64Path,coalNum,srcDataPath,batchTime)
      }
      if(sum64To42 > 2 && sum64To42 % 2==1){
        coalNum = sum64To42/2 + 1
        setToDir(fileSizesLess64Set,fileSizeLess64Path,mergeSrc64Path,mergeData64Path,coalNum,srcDataPath,batchTime)
      }
      //文件大小42~25
      if(sum42To25 > 1 && sum42To25 <= 3){
        coalNum = 1
        setToDir(fileSizesLess42Set,fileSizeLess42Path,mergeSrc42Path,mergeData42Path,coalNum,srcDataPath,batchTime)
      }
      if(sum42To25 > 3){
        coalNum = sum64To42/3 + 1
        setToDir(fileSizesLess42Set,fileSizeLess42Path,mergeSrc42Path,mergeData42Path,coalNum,srcDataPath,batchTime)
      }
      //文件大小 < 25
      if(sumLess25 > 1){
        coalNum = 1
        setToDir(fileSizesLess25Set,fileSizeLess25Path,mergeSrc25Path,mergeData25Path,coalNum,srcDataPath,batchTime)
      }


    }catch {
      case e:Exception => {
        e.printStackTrace()
        mergeInfo = "merge failed"
      }
    }

    def setToDir(set: mutable.HashSet[String],fileSizeLessNumPath:String,mergeSrcNumPath:String,
                 mergeDataNumPath:String,coalNum:Int,srcDataPath:String,batchTime:String) : Unit = {
      //遍历set , 写入临时目录
      set.foreach(f => {
        //println(f)
        val fileName = f.substring(f.lastIndexOf("/") + 1)
        val fromPath = new Path(f)

        var destLocation = f.replace(srcDataPath, fileSizeLessNumPath)
        val fileSuffix = if (fileName.contains(".")) fileName.substring(fileName.lastIndexOf(".")) else ""
        val newFileName = batchTime + "_" + num + fileSuffix

        destLocation = destLocation.substring(0, destLocation.lastIndexOf("/") + 1) + newFileName
          num = num + 1
        val destPath = new Path(destLocation)

        if (!fileSystem.exists(destPath.getParent)) {
          fileSystem.mkdirs(destPath.getParent)
        }
        fileSystem.rename(fromPath, destPath)
      })

      // 将需要合并的文件mv到临时目录
      FileUtils.moveFiles(fileSystem, batchTime, fileSizeLessNumPath, mergeSrcNumPath, true)
      val srcDF = sqlContext.read.format("orc").load(mergeSrcNumPath + "/")
      // 将合并目录的src子目录下的文件合并后保存到合并目录的data子目录下
      srcDF.coalesce(coalNum).write.format("orc").mode(SaveMode.Overwrite).save(mergeDataNumPath)
      // 将合并目录的data目录下的文件移动到原目录
      FileUtils.moveFiles(fileSystem, batchTime, mergeDataNumPath, srcDataPath, false)
      // 删除 合并目录src的子目录
      fileSystem.delete(new Path(mergePath + "/" + batchTime), true)
    }

    mergeInfo
  }

}
