package com.viswa.cloud

import org.apache.spark.sql.SparkSession

class SparkSessionUtils{

  val spark =
    SparkSession
      .builder()
      .master("local[*]") //YARN
      .config("spark.app.name", "team_DE")
      .getOrCreate()

  val x= 10
}
