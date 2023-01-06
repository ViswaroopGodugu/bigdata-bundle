package com.viswa.cloud

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

case class PeopleData(SNO: String = "", NAME: String = "", AGE: String = "", ID: String = "") {}

case class CountryData(var COUNTRY: String = "", var ID: String = "") {}

object SparkJoinsWithDataSets extends App {

  val logger = LoggerFactory.getLogger("SQLOperations")

  val spark =
    SparkSession
      .builder()
      .master("local[*]") //YARN
      .config("spark.app.name", "team_DE")
      .getOrCreate()

  spark.sparkContext.setLogLevel("OFF")

  val df1 = spark.createDataFrame(
    Seq(
      Tuple2("IN", "0001"),
      Tuple2("USA", "0002"))
  ).toDF("COUNTRY", "ID")

  val df2 = spark.createDataFrame(
    Seq(
      Tuple4("1", "ABC", "25", "0001"),
      Tuple4("2", "XYZ", "39", "0002"),
      Tuple4("3", "CBZ", "29", "004"))
  ).toDF("SNO", "NAME", "AGE", "ID")

  df1.show
  df2.show
  df1.createOrReplaceTempView("tabl1")
  df2.createOrReplaceTempView("tabl2")

  spark.catalog.listTables().show()

  //  spark.sql(
  //    """
  //      | select * from tabl1 a left join tabl2 b on a.ID==b.ID
  //      |
  //      |""".stripMargin
  //  ).show()

  //join with Expression
  //  df1.as("a")
  //    .join(
  //      df2.as("b")
  //      , expr("a.ID==b.ID")
  //      , "left"
  //    ).show()

  //join with Seq  == inner
  //  df1.as("a")
  //    .join(
  //      df2.as("b")
  //      , Seq("ID")
  //      , "inner"
  //    ).show()

  //join with Seq  == left
  //  df1.as("a")
  //    .join(
  //      df2.as("b")
  //      , Seq("ID")
  //      , "left"
  //    ).show()

  df2.as("a")
    .join(
      df1.as("b")
      , Seq("ID")
      , "right"
    ).show()

  println("------- With DS -----------------------")

  import spark.implicits._

  val ds1 = df1.as[CountryData]
  val ds2 = df2.as[PeopleData]

  ds2.joinWith(ds1, ds1("ID") === ds2("ID"), "inner").show
  ds2.joinWith(ds1, ds1("ID") === ds2("ID"), "inner").printSchema()
  ds2.join(ds1, ds1("ID") === ds2("ID"), "inner").show

  println("===>"+CountryData().ID)

  ds2.join(ds1, expr(s"${CountryData().ID} == ${PeopleData().ID}"), "inner").show

  println("------- PROGRAM END ------------------------")
}
//output:
//+-------+----+
//|COUNTRY|  ID|
//+-------+----+
//|     IN|0001|
//|    USA|0002|
//+-------+----+
//
//+---+----+---+----+
//|SNO|NAME|AGE|  ID|
//+---+----+---+----+
//|  1| ABC| 25|0001|
//|  2| XYZ| 39|0002|
//|  3| CBZ| 29| 004|
//+---+----+---+----+
//
//+-----+--------+-----------+---------+-----------+
//| name|database|description|tableType|isTemporary|
//+-----+--------+-----------+---------+-----------+
//|tabl1|    null|       null|TEMPORARY|       true|
//|tabl2|    null|       null|TEMPORARY|       true|
//+-----+--------+-----------+---------+-----------+
//
//+-------+----+---+----+---+----+
//|COUNTRY|  ID|SNO|NAME|AGE|  ID|
//+-------+----+---+----+---+----+
//|     IN|0001|  1| ABC| 25|0001|
//|    USA|0002|  2| XYZ| 39|0002|
//+-------+----+---+----+---+----+
//
//+-------+----+---+----+---+----+
//|COUNTRY|  ID|SNO|NAME|AGE|  ID|
//+-------+----+---+----+---+----+
//|     IN|0001|  1| ABC| 25|0001|
//|    USA|0002|  2| XYZ| 39|0002|
//+-------+----+---+----+---+----+
//
//------- PROGRAM END ------------------------