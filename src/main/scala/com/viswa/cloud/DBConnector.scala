package com.viswa.cloud

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import java.util.Properties

object DBConnector {
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

    //jdbc:postgresql://localhost/db?user=postgres&password=password
    //    //5432

    val jdbcDF = spark.read
      .format("jdbc")
      //.option("url", "jdbc:postgresql://localhost:5432/db?user=postgres&password=password")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/db?user=postgres&password=password")
      .option("dbtable", "public.acc_balance2")
      .option("user", "postgres")
      .option("password", "password")
      .load()

    jdbcDF.show()

  }
}
