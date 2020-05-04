package org.apache.spark.examples.Utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GcsFileOperation {

  def getSparkContxt: SparkContext={
    val spark = SparkSession.builder.appName("Read and write with schema").getOrCreate()
    val sc=spark.sparkContext
    sc

  }

  def getGcsFileInfo(BucketName: String): scala.collection.mutable.Map[String,Int]=
  {
    val p = BucketName
    val path = new Path(p)
    val fs = path.getFileSystem(getSparkContxt.hadoopConfiguration)
    val files = fs.listStatus(path)
    var fileInfo = scala.collection.mutable.Map[String, Int]()
    files.map(_.getPath().toString).foreach { p=>
      val key=p
      val len=fs.getLength(new Path(p)).toInt
      fileInfo +=(p -> len)
    }
    fileInfo
  }

  def removeGcsFile(file : String) ={
    val path = new Path(file)
    val fs = path.getFileSystem(getSparkContxt.hadoopConfiguration)
    val files = fs.listStatus(path)
    if (fs.exists(path))
      fs.delete(path, true)
  }

}
