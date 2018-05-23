package com.zyuc.dpi.basic

import java.util
import com.zyuc.dpi.hdfs.{HdfsFile, HdfsFileSizeComparator}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by zhoucw on 18-5-20 下午5:51.
  * @deprecated
  */
object FileManage1 {
  def main(args: Array[String]): Unit = {
  //  val spark = SparkSession.builder().enableHiveSupport().appName("name_123-201805221210").master("local[3]").getOrCreate()
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath", "/hadoop/test/output/data/")
    val mergePath = sc.getConf.get("spark.app.mergePath", "/hadoop/test/output/merge/")
    val filterMaxSize =  sc.getConf.get("spark.app.sizeUpLimit", "80") // 上限
    val singleFileSize = sc.getConf.get("spark.app.singleFileSize", "120") // 合并后的文件大小
    val ifDeleteTmp = sc.getConf.get("spark.app.ifDeleteTmp", "1")
    val singFileMaxSize = singleFileSize.toInt * 1024 * 1024

    val hid = appName.substring(appName.lastIndexOf("_")+1, appName.lastIndexOf("-"))
    val dataTime = appName.substring(appName.lastIndexOf("-")+1)
    val batchid = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val h = dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val filePath = inputPath + s"hid=${hid}/d=${d}/h=${h}/m5=${m5}"
    println("filePath:" + filePath)
    val fileGroups = genFileGroups(fileSystem, filePath, filterMaxSize.toInt, singFileMaxSize)

    mergeFiles(spark, fileGroups, fileSystem, filePath, mergePath, batchid, ifDeleteTmp)

  }

  def genFileGroups(fileSystem: FileSystem, filePath: String, filterMaxSize:Int, singFileMaxSize:Long): Array[HdfsFile] = {

    val treeSet: util.TreeSet[HdfsFile] = new util.TreeSet[HdfsFile](new HdfsFileSizeComparator("desc"))

    fileSystem.globStatus(new Path(filePath + "/*")).foreach(x => {
      if(x.getLen < filterMaxSize * 1024 * 1024){
        treeSet.add(new HdfsFile(x.getPath.getName, x.getLen))
      }
    }
    )
    val newTreeSet = new util.TreeSet[HdfsFile](new HdfsFileSizeComparator("desc"))

    val iterator = treeSet.iterator
    while (iterator.hasNext) {
      HdfsFile.setGroup(newTreeSet, iterator.next, singFileMaxSize)
    }

    println("newTreeSet:" + newTreeSet)

    val fileGroups = new util.ArrayList[HdfsFile]
    val newIterator = newTreeSet.iterator()
    while(newIterator.hasNext){
      val file = newIterator.next();
      if(file.getName.split("#").length>1){
        fileGroups.add(file)
      }
    }

    val fileArr = new Array[HdfsFile](fileGroups.size())
    fileGroups.toArray(fileArr)
    fileArr
  }

  def mergeFiles(spark: SparkSession, fileGroups: Array[HdfsFile], fileSystem: FileSystem, filePath: String, mergePath: String, batchId: String, ifDeleteTmp:String): Unit = {
    var num = 0
    val mergeTmpPath = mergePath + "/" + batchId + "/tmp"
    val mergeDataPath = mergePath + "/" + batchId + "/data"

    if(fileGroups.length<1){
      println("no files for merge")
      return
    }

    fileGroups.map(x => x.getName.split("#")).foreach(g => {
      g.foreach(f => {
        val srcPath = new Path(filePath + "/" + f)
        val targePath = new Path(mergeTmpPath + "/" + num + "/" + f)
        if (!fileSystem.exists(targePath.getParent)) {
          fileSystem.mkdirs(targePath.getParent)
        }
        fileSystem.rename(srcPath, targePath)
      })
      num = num + 1
    }
    )

    var df: DataFrame = null
    fileSystem.globStatus(new Path(mergeTmpPath + "/*")).foreach(p => {
      println(p.getPath)
      if (df == null) {
        df = spark.read.format("orc").load(p.getPath.toString)
        df = df.coalesce(1)
      } else {
        val tmpDF = spark.read.format("orc").load(p.getPath.toString).coalesce(1)
        df = df.union(tmpDF)
      }
    })


    df.write.format("orc").mode(SaveMode.Overwrite).save(mergeDataPath)

    var index = 0;
    fileSystem.globStatus(new Path(mergeDataPath + "/*.orc")).foreach(f => {
      println(f.getPath.getName)
      val targePath = new Path(filePath + "/merge_" + batchId + "_" + index + ".orc")
      fileSystem.rename(f.getPath, targePath)
      index = index + 1
    })

    // 删除临时目录
    if(ifDeleteTmp == "1"){
      fileSystem.globStatus(new Path(mergePath + "/" + batchId)).foreach(x=>{
        fileSystem.delete(x.getPath, true)
      })
    }

  }
}
