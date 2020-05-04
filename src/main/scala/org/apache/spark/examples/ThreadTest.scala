package org.apache.spark.examples

import java.nio.file.{Files, Paths}

import com.google.cloud.spark.bigquery._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.col
import java.sql.Timestamp

import org.apache.spark.examples.Utils.{GcsFileOperation, readFileFromResource}

import scala.concurrent.ExecutionContext.Implicits.global
import org.elasticsearch.spark.sql._
import org.apache.spark.examples.Utils.readFileFromResource

import scala.concurrent.Future

object ThreadTest {

  def demo1()={

    val conf: SparkConf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().appName("WriteToES").config(conf).config("spark.es.nodes.discovery", "false").config("spark.es.cluster.name","kibana-cluster").config("spark.es.port","9200").config("es.nodes.wan.only","true").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("temporaryGcsBucket", "commonbuk")
    spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "34.87.139.247:9094")
      .option("subscribe", "demo")
      .option("startingOffsets", "latest") //latest
      .load()

    val jsonFormatSchema = new String(
      Files.readAllBytes(Paths.get(readFileFromResource.readFromResource("/schema/sam.avsc").getAbsolutePath)))


    val personDF = df.select(from_avro(col("value"), jsonFormatSchema).as("person"))
      .select("person.*")

    personDF.writeStream.option("checkpointLocation", "gs://commonbuk/demo1")
      .foreachBatch {
        (df: Dataset[Row], _: Long) =>
          df.write.mode("append").format("csv").save("gs://tab1/firstopic")
          GcsFileOperation.getGcsFileInfo("gs://tab1/firstopic")
            .foreach { p=>
              println("my value is",p._2)
              if (p._2 != 0) {

                val firstdf = spark.read.csv("gs://tab1/firstopic")
                println(firstdf.show(3))

                val firstdf1 = firstdf.withColumnRenamed("_c0", "id")
                  .withColumnRenamed("_c1", "name")
                  .withColumnRenamed("_c2", "email")
                  .withColumnRenamed("_c3", "sal")

                firstdf1.write
                  .option("checkpointLocation", "gs://commonbuk/big1")
                  .option("table", "coastal-mercury-275507.mydata.mycsv")
                  .format("bigquery")
                  .mode("append")
                  .save()

                GcsFileOperation.removeGcsFile(p._1)
              }
              else {
                GcsFileOperation.removeGcsFile(p._1)
              }}
      }.start()
  }

  def demo2()={

    val conf: SparkConf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().appName("WriteToES").config(conf).config("spark.es.nodes.discovery", "false").config("spark.es.cluster.name","kibana-cluster").config("spark.es.port","9200").config("es.nodes.wan.only","true").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("temporaryGcsBucket", "commonbuk")
    spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    val df1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "34.87.139.247:9094")
      .option("subscribe", "demo1")
      .option("startingOffsets", "latest") //latest
      .load()

    val jsonFormatSchema = new String(
      Files.readAllBytes(Paths.get(readFileFromResource.readFromResource("/schema/sam1.avsc").getAbsolutePath)))


    val personDF1 = df1.select(from_avro(col("value"), jsonFormatSchema).as("person1"))
      .select("person1.*")

    personDF1.writeStream.option("checkpointLocation", "gs://commonbuk/demo2")
      .foreachBatch {
        (df: Dataset[Row], _: Long) =>
          df.write.mode("append").format("csv").save("gs://tab2/sectopic")
          GcsFileOperation.getGcsFileInfo("gs://tab2/sectopic")
            .foreach { p=>
              println("my second topic value is",p._2)
              if (p._2 != 0) {

                val firstdf2 = spark.read.csv("gs://tab2/sectopic")
                println(firstdf2.show(3))

                val firstdf3 = firstdf2.withColumnRenamed("_c0", "id")
                  .withColumnRenamed("_c1", "name")

                firstdf3.write
                  .option("checkpointLocation", "gs://commonbuk/big2")
                  .option("table", "coastal-mercury-275507.mydata.mysec")
                  .format("bigquery")
                  .mode("append")
                  .save()

                GcsFileOperation.removeGcsFile(p._1)
              }
              else {
                GcsFileOperation.removeGcsFile(p._1)
              }}
      }.start()
  }

  def main(args: Array[String]): Unit = {

    while(true) {
      val f1 = Future {
        demo1()
      } //work starts here
      val f2 = Future {
        demo2()
      } //and here

      val result = for {
        r1 <- f1
        r2 <- f2

      } yield (r1, r2)

      // important for a little parallel demo: keep the jvm alive
      sleep(20)

      def sleep(time: Long): Unit = Thread.sleep(time)
    }
  }
}
