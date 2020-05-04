package org.apache.spark.examples

import java.nio.file.{Files, Paths}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Dataset, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.concurrent.Future


object App1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[*]")

    val spark = SparkSession.builder().appName("WriteToES").config(conf).getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val Data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("gs://mysrcfile/data.csv")
    Data.show(2)

    Data.select(to_avro(struct("*")) as "value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "35.184.158.71:9094")
      .option("kafka.request.required.acks", "1")
      .option("topic", "demo1")
      .save()

  }

}
