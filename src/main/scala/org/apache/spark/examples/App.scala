package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.struct

object App extends App {

  val conf: SparkConf = new SparkConf()
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //conf.set("spark.driver.bindAddress", "127.0.0.1")
 // conf.setMaster("k8s://https://34.71.176.218:443")
  val spark = SparkSession.builder.appName("Read and write with schema").config(conf).getOrCreate()

  val sampleData = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("gs://srcfilepath/sampledata.csv")
  sampleData.show(2)

  sampleData.select(to_avro(struct("*")) as "value").write.format("kafka")
    .option("kafka.bootstrap.servers", "34.87.139.247:9094")
    .option("kafka.request.required.acks", "1")
    .option("topic", "demo").save()

    val Data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("gs://srcfilepath/Data.csv")
   Data.show(2)

      Data.select(to_avro(struct("*")) as "value")
     .write
     .format("kafka")
     .option("kafka.bootstrap.servers", "34.87.139.247:9094")
     .option("kafka.request.required.acks", "1")
     .option("topic", "demo1")
     .save()

  spark.sparkContext.stop()

}