package com.viswa.cloud

import org.apache.spark.sql.SparkSession

trait SparkUtilsTrait {

  def getSaprkSession(abc: String): SparkSession = {
    val spark =
      SparkSession
        .builder()
        .master("local[*]") //YARN
        .config("spark.app.name", "team_DE")
        .getOrCreate()
    spark
  }

  def applyBusinessLogic(): Unit ={

  }

  def getSparkSession(): Unit = {

  }


  val x = 10
}
