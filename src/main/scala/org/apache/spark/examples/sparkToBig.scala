package org.apache.spark.examples

import java.nio.file.{Files, Paths}

import com.google.cloud.spark.bigquery._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.col
import java.sql.Timestamp

import org.apache.spark.examples.Utils.{GcsFileOperation, readFileFromResource}
import org.elasticsearch.spark.sql._
import org.apache.spark.examples.Utils.readFileFromResource

object sparkToBig {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[*]")

    val spark = SparkSession.builder().appName("WriteToES").config(conf).config("spark.es.nodes.discovery", "false").config("spark.es.cluster.name","kibana-cluster").config("spark.es.port","9200").config("es.nodes.wan.only","true").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("temporaryGcsBucket", "commonbuk")
    spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")


    //content of this dataframe will be in binary format
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "35.189.173.183:9094")
      .option("subscribe", "demo")
      .option("startingOffsets", "latest") //latest
      .load()

    val jsonFormatSchema = new String(
      Files.readAllBytes(Paths.get(readFileFromResource.readFromResource("/schema/sam.avsc").getAbsolutePath)))


    val personDF = df.select(from_avro(col("value"), jsonFormatSchema).as("person"))
      .select("person.*")

    personDF.writeStream.option("checkpointLocation", "gs://commonbuk/")
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
      }.start().awaitTermination()

    /* personDF.writeStream
      .format("csv")
      .outputMode("append")
      .option("checkpointLocation", "gs://shuvabucket/df")
      .option("path", "gs://mytab1/firstopic")
      .start().awaitTermination(120000)

          val firstdf = spark.read.csv("gs://mytab1/firstopic")
          println(firstdf.show(3))

          val firstdf1= firstdf.withColumnRenamed("_c0","id")
            .withColumnRenamed("_c1","name")
            .withColumnRenamed("_c2","email")
            .withColumnRenamed("_c3","sal")

             firstdf1.printSchema()

             firstdf1.write
            .option("checkpointLocation", "gs://shuvabucket/big1")
            .option("table","serene-radius-275116.mydata.mycsv")
            .format("bigquery")
            .mode("append")
            .save()
     */


  }


}
