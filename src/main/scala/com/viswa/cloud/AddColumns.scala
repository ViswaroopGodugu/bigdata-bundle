package com.viswa.cloud

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object AddColumns extends Logging with App {

  val logger = LoggerFactory.getLogger("SQLOperations")

  val spark =
    SparkSession
      .builder()
      .master("local[*]") //YARN
      .config("spark.app.name", "team_DE")
      .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  spark
    .read
    .option("header", true)
    .csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\Trans.csv.txt")
    .withColumn("updates_AMT", col("AMOUNT").multiply("1.1"))
    .withColumn("STD_TEMP", lit(95))
    .withColumn("Deviates", col("AMOUNT") - col("STD_TEMP"))
    .withColumn("duplicared", col("AMOUNT"))
    .withColumn("calCal", expr("concat(updates_AMT,'  **   ',ACC_NUM)"))
    .show()

  //union and unionAll

  val trans1 = spark
    .read
    .option("header", true)
    .csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\Trans.csv.txt")

  val trans2 = spark
    .read
    .option("header", true)
    .csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\Trans2.txt")

  trans1.show()
  trans2.show()

  println("==============================================================")

  trans1.union(trans2).show()
  trans1.union(trans2).distinct().show()
}
