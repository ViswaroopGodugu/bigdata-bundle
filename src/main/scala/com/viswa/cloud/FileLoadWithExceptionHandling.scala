package com.viswa.cloud

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object FileLoadWithExceptionHandling extends Logging with App {

  val logger = LoggerFactory.getLogger("SQLOperations")

  val spark =
    SparkSession
      .builder()
      .master("local[*]") //YARN
      .config("spark.app.name", "team_DE")
      .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val transDF = spark.read.option("header", true).csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\Trans.csv.txt")

  val ratesDF: DataFrame =
    try {
      spark.read.option("header", true).csv("D:\\Batch1\\CurrecnyExchange\\ExchrangeRates\\rates_.csv.txt")
    } catch {
      case eX: Throwable => {
        logger.error("Exchange rates data is Not loaded ")
        throw new Exception("Exchange rates data is Not loaded ")
        spark.emptyDataFrame
      }
    }finally {
      println("program ended ")
    }

  transDF.show()
  ratesDF.show()

  transDF.as("a")
    .join(
      ratesDF.as("b"),
      expr("a.CCY == b.baseCurrency"),
      "leftouter"
    )
    .select("a.DATE", "a.ACC_NUM", "a.CCY", "a.AMOUNT", "b.FCurrency", "b.Mutilply")
    .withColumn("FamtNew", expr("CASE WHEN CCY=='USD' THEN AMOUNT  ELSE  AMOUNT/Mutilply  END"))
    .show

}
