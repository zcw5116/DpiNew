package com.zyuc.dpi.basic

import java.util
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.zyuc.dpi.hdfs.{HdfsFile, HdfsFileSizeComparator}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConversions._
import org.apache.spark.sql.functions.lit

import scala.collection.mutable

/**
  * Created by zhoucw on 18-5-20 下午5:51.
  */
object FileManage {
  def main(args: Array[String]): Unit = {
    // val spark = SparkSession.builder().enableHiveSupport().appName("name_1-201805221210").master("local[3]").getOrCreate()
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val appName = sc.getConf.get("spark.app.name") // 格式： name_第几次合并_待合并的时间目录
    val inputPath = sc.getConf.get("spark.app.inputPath", "/hadoop/test/output/data/")
    val mergePath = sc.getConf.get("spark.app.mergePath", "/hadoop/test/output/merge/")
    val houseids = sc.getConf.get("spark.app.houseids", "123,124")
    val filterMaxSize = sc.getConf.get("spark.app.sizeUpLimit", "80") // 上限
    val singleFileSize = sc.getConf.get("spark.app.singleFileSize", "120") // 合并后的文件大小
    val ifDeleteTmp = sc.getConf.get("spark.app.ifDeleteTmp", "1")
    val singFileMaxSize = singleFileSize.toInt * 1024 * 1024

    val hid = appName.substring(appName.lastIndexOf("_") + 1, appName.lastIndexOf("-"))
    val dataTime = appName.substring(appName.lastIndexOf("-") + 1)
    val batchid = appName.substring(appName.lastIndexOf("_") + 1)
    val d = dataTime.substring(2, 8)
    val h = dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)

    val houseArr = houseids.split(",")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val filePath = inputPath
    val fileGroupsMap = genFileGroups(fileSystem, filePath, dataTime, houseArr, filterMaxSize.toInt, singFileMaxSize)

    mergeFiles(spark, fileGroupsMap, fileSystem, filePath, dataTime, houseArr, mergePath, batchid, ifDeleteTmp)

  }

  def genFileGroups(fileSystem: FileSystem, filePath: String, dataTime: String, houseArr: Array[String], filterMaxSize: Int, singFileMaxSize: Long): util.HashMap[String, util.ArrayList[HdfsFile]] = {

    val d = dataTime.substring(2, 8)
    val h = dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)
    val partitionPath = s"d=${d}/h=${h}/m5=${m5}"

    val treeSet: util.TreeSet[HdfsFile] = new util.TreeSet[HdfsFile](new HdfsFileSizeComparator("desc"))

    // val houseArr = Array("1002", "1005","1018","1019","1022","1023", "237","240","241","242","246")

    def getTreeSet(hid: String): util.TreeSet[HdfsFile] = {
      val tmpTree: util.TreeSet[HdfsFile] = new util.TreeSet[HdfsFile](new HdfsFileSizeComparator("desc"))
      fileSystem.globStatus(new Path(filePath + s"/hid=${hid}/${partitionPath}/*")).foreach(x => {
        if (x.getLen < filterMaxSize * 1024 * 1024) {
          tmpTree.add(new HdfsFile(x.getPath.getName, x.getLen))
        }
      })
      tmpTree
    }

    val map = new util.HashMap[String, util.TreeSet[HdfsFile]]()
    houseArr.foreach(hid => {
      map.put(hid, getTreeSet(hid))
    })


    // 对每个机房的数据装箱分组
    val newMap = new util.HashMap[String, util.TreeSet[HdfsFile]]()
    val mapIterator = map.entrySet().iterator()
    while (mapIterator.hasNext) {
      val hTreeMap = mapIterator.next()
      val hid = hTreeMap.getKey
      val hTreeSet = hTreeMap.getValue
      val newTreeSet = new util.TreeSet[HdfsFile](new HdfsFileSizeComparator("desc"))
      val iterator = hTreeSet.iterator
      var singleMaxSize = singFileMaxSize
      if (hid == "1002") {
        singleMaxSize = 80 * 1024 * 1024
      }
      while (iterator.hasNext) {
        HdfsFile.setGroup(newTreeSet, iterator.next, singleMaxSize)
      }
      newMap.put(hid, newTreeSet)
    }


    // 过滤不需要合并的文件
    val mapGroups = new util.HashMap[String, util.ArrayList[HdfsFile]]()
    newMap.foreach(m => {
      val hid = m._1
      val tree = m._2
      val fileGroups = new util.ArrayList[HdfsFile]
      tree.foreach(t => {
        if (t.getName.contains("#")) {
          fileGroups.add(t)
        }
      })

      if (fileGroups.size() > 0) {
        mapGroups.put(hid, fileGroups)
      }
    })

    mapGroups
  }


  def mergeFiles(spark: SparkSession, fileGroupsMap: util.HashMap[String, util.ArrayList[HdfsFile]], fileSystem: FileSystem, filePath: String, dataTime: String, houseArr: Array[String], mergePath: String, batchId: String, ifDeleteTmp: String): Unit = {

    val mergeTmpPath = mergePath + "/" + batchId + "/tmp"
    val mergeDataPath = mergePath + "/" + batchId + "/data"

    val d = dataTime.substring(2, 8)
    val h = dataTime.substring(8, 10)
    val m5 = dataTime.substring(10, 12)
    val partitionPath = s"d=${d}/h=${h}/m5=${m5}"


    def renameFile2TmpByHid(hid: String): Unit = {
      val fileGroups = fileGroupsMap.get(hid)
      if (fileGroups == null) {
        return
      }
      var num = 0
      fileGroups.map(x => x.getName.split("#")).foreach(g => {
        g.foreach(f => {
          val srcPath = new Path(filePath + "/hid=" + hid + "/" + partitionPath + "/" + f)
          val targePath = new Path(mergeTmpPath + "/hid=" + hid + "/" + num + "/" + f)
          if (!fileSystem.exists(targePath.getParent)) {
            fileSystem.mkdirs(targePath.getParent)
          }
          fileSystem.rename(srcPath, targePath)
        })
        num = num + 1
      })
    }

    def getDataFrameByHid(hid: String): DataFrame = {
      var df: DataFrame = null
      fileSystem.globStatus(new Path(mergeTmpPath + "/hid=" + hid + "/*")).foreach(p => {
        if (df == null) {
          df = spark.read.format("orc").load(p.getPath.toString)
          df = df.coalesce(1)
        } else {
          val tmpDF = spark.read.format("orc").load(p.getPath.toString).coalesce(1)
          df = df.union(tmpDF)
        }
      })

      if (df != null) {
        df = df.withColumn("hid", lit(hid))
      }
      df
    }


    val dfSet = new mutable.HashSet[DataFrame]()

    // 使用多线程加载dataFrame
    val executor = Executors.newFixedThreadPool(30)
    for (hid <- houseArr) {
      executor.submit(new Runnable {
        override def run(): Unit = {
          renameFile2TmpByHid(hid)
          val hdf = getDataFrameByHid(hid)
          if (hdf != null) {
            dfSet.add(hdf)
          }
        }
      })
    }
    executor.shutdown()
    executor.awaitTermination(Long.MaxValue, TimeUnit.MINUTES)

    var df: DataFrame = null
    dfSet.iterator.foreach(d => {
      if (df == null) {
        df = d
      } else {
        df = df.union(d)
      }
    })


    /*   var df:DataFrame = null
       houseArr.foreach(hid=>{
         renameFile2TmpByHid(hid)
         if(df == null){
           df = getDataFrameByHid(hid)
         }else{
           val hdf = getDataFrameByHid(hid)
           if(hdf != null){
             df = df.union(hdf)
           }
         }
       })
   */
    //  df.show()


    df.write.format("orc").mode(SaveMode.Overwrite).partitionBy("hid").save(mergeDataPath)

    def renameFile2DataByHid(hid: String) = {
      var index = 0;
      fileSystem.globStatus(new Path(mergeDataPath + "/hid=" + hid + "/*.orc")).foreach(f => {
        println(f.getPath.getName)
        val targePath = new Path(filePath + "/hid=" + hid + "/" + partitionPath + "/merge_" + batchId + "_" + index + ".orc")
        fileSystem.rename(f.getPath, targePath)
        index = index + 1
      })
    }

    houseArr.foreach(hid => {
      renameFile2DataByHid(hid)
    })

    // 删除临时目录
    if (ifDeleteTmp == "1") {
      fileSystem.globStatus(new Path(mergePath + "/" + batchId)).foreach(x => {
        fileSystem.delete(x.getPath, true)
      })
    }

  }
}
