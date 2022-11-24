package com.viswa.cloud

import org.apache.spark.sql.SparkSession

object SparkSessionUtils {

  val spark =
    SparkSession
      .builder()
      .master("local[*]") //YARN
      .config("spark.app.name", "team_DE")
      .getOrCreate()


}
