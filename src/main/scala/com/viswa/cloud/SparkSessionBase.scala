package com.viswa.cloud

import org.apache.hadoop.fs.PathNotFoundException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max, Min}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.WindowSpecContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import java.io.FileNotFoundException

class SparkSessionBase extends Logging with App {

  val logger = LoggerFactory.getLogger("SQLOperations")

  val spark =
    SparkSession
      .builder()
      .master("local[*]") //YARN
      .config("spark.app.name", "team_DE")
      .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

}
