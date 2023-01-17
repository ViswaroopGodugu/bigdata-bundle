package com.viswa.cloud

import .spark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object StreemingSaprk extends Logging with App {
  val logger = LoggerFactory.getLogger("SQLOperations")
  val spark =
    SparkSession
      .builder()
      .master("local[*]") //YARN
      .config("spark.app.name", "team_DE")
      .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val queryName = "my_stream_test"

  //read options
  val csvDF = spark
    .readStream
    .option("sep", ";")
    .option("maxFilesPerTrigger", "1")
    .schema(
      new StructType()
        .add("value", "string")
    )
    .csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\*")

  //write options
  val streamQuerymanger = csvDF
    .writeStream
    .queryName(queryName)
    .option("checkpointLocation", s"D:\\CHECKPOINT\\${queryName}\\")
    .foreachBatch((inputData: DataFrame, microMatchCounter: Long) => {

      println(s"=================================${microMatchCounter}============================================")
      inputData.withColumn("inputFileName", input_file_name()).show(false)
      println(s"=================================end of ${microMatchCounter}============================================")

    })

  //  val streamQuerymanger =
  //    csvDF
  //      .repartition(2)
  //      .withColumn("filename",input_file_name())
  //      .writeStream
  //      .outputMode("append")
  //      .format("parquet")
  //      .option("path", "D:\\DATA\\STRAM_OUTPUT")
  //      .queryName(queryName)
  //      .option("checkpointLocation", s"D:\\CHECKPOINT\\${queryName}\\")

  streamQuerymanger.start()



  spark.streams.awaitAnyTermination()
}
