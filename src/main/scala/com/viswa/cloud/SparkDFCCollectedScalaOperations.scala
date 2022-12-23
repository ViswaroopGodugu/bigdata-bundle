package com.viswa.cloud

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object SparkDFCCollectedScalaOperations extends App {

  val logger = LoggerFactory.getLogger("SQLOperations")

  val spark =
    SparkSession
      .builder()
      .master("local[*]") //YARN
      .config("spark.app.name", "team_DE")
      .getOrCreate()

  spark.sparkContext.setLogLevel("OFF")

  val df2 = spark.createDataFrame(
    Seq(
      Tuple5("1", "ABC", "25", "0001", "abc@gmail.com"),
      Tuple5("2", "XYZ", "39", "0002", "asdf@gmail.com"),
      Tuple5("3", "CBZ", "29", "004", "xyz@gmail.com"))
  ).toDF("SNO", "NAME", "AGE", "ID", "email")

  //--driver-memory 4GB
  //--executor-memory 4GB
  //--num-executors 2

  df2.show()

  //df2 sze is 5 GB
  df2.show(20)
  df2.show(df2.count().toInt)
  //driver Out of memory exception

  //collect is for convert data from dataframe to List
  val scalaListRows = df2.collect()
  //driver Out of memory exception

  //foreach just to reach each line and apply the logic
  scalaListRows.foreach(x => {
    val emailID = x.get(4).asInstanceOf[String]
    println(s"sending email to ---${emailID} ")
  })

  //map will create new set from existing set
  val scalaListRows2 = scalaListRows.map(x => {
    x.get(4).asInstanceOf[String].concat("**************")
  })
  scalaListRows2.foreach(x => println(x))

  //filter the rows based on prefix
  val scalaListRows3 = scalaListRows.filter(x => {
    val id = x.get(4).asInstanceOf[String]
    if (id.asInstanceOf[String].startsWith("a")) {
      println(s"adding items to scalaListRows3 --- ${id}")
      true
    } else false
  })
  scalaListRows3.foreach(x => println(x))

  df2.filter(expr("email like 'a%' ")).show

}