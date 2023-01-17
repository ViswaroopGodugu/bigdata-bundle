package com.viswa.cloud

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lit, when}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

object ColumnOperatitions {
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

    val df = spark
      .read
      .option("header", true)
      .csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\Trans.csv.txt")

    df.show
    //+--------+-------+---+------+--------+
    //|    DATE|ACC_NUM|CCY|AMOUNT|Mutilply|
    //+--------+-------+---+------+--------+
    //|20221215|  12345|INR|   500|      aa|
    //|20221214|   8682|USD|    70|      vv|
    //+--------+-------+---+------+--------+
    val df2 =
    df
      .withColumn("CCY_updated", expr("concat(CCY,'_currency')"))
      .withColumn("CCY_updated_API", concat(col("CCY"),lit("_currency")))
      .withColumn("Stand_value", lit(81))
      .withColumn("equel_USD", col("AMOUNT").divide(col("Stand_value")))
      .withColumn("equel_USD_new", when(col("CCY") === "USD", col("AMOUNT")).otherwise(col("equel_USD")))
      .withColumn("equel_USD_new_new",
        expr("case when CCY=='USD' THEN AMOUNT ELSE equel_USD END"))

    df2.show
    //+--------+-------+---+------+--------+------------+---------------+-----------+------------------+-------------+
    //|    DATE|ACC_NUM|CCY|AMOUNT|Mutilply| CCY_updated|CCY_updated_API|Stand_value|         equel_USD|equel_USD_new|
    //+--------+-------+---+------+--------+------------+---------------+-----------+------------------+-------------+
    //|20221215|  12345|INR|   500|      aa|INR_currency|   INR_currency|         81| 6.172839506172839|        6.172|
    //|20221214|   8682|USD|    70|      vv|USD_currency|   USD_currency|         81|0.8641975308641975|           70|
    //+--------+-------+---+------+--------+------------+---------------+-----------+------------------+-------------+

  }
}
