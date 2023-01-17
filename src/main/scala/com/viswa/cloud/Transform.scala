package com.viswa.cloud

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object Transform {
  val logger = LoggerFactory.getLogger("SQLOperations")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "team_DE")
    sparkConf.setMaster("local[*]")
    val spark =
      SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    println("---------Spark session is ready----------------------" + spark)

    val df = spark.createDataFrame(Seq(
      Tuple6("ABC", "909090", "11: AM", "CREDIT", "10000", " Salary credited"),
      Tuple6("ABC", "909090", "11: AM", "DEBIT", "5", " Salary credited"),
    ))
      .toDF("USERNAME", "ACC_ID", "TIMESTAMP", "TRANTYPE", "AMOUNT", "DESCRIPTIONS")

    df.show
  }
}
