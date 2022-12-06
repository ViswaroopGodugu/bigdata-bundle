package com.viswa.cloud

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Max, Min}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.WindowSpecContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SQLOperations extends App {

  val spark =
    SparkSession
      .builder()
      .master("local[*]") //YARN
      .config("spark.app.name", "team_DE")
      .getOrCreate()

  //load data
  spark.sparkContext.setLogLevel("OFF")

  val dataframe1 = spark.read.csv("D:\\Batch1\\stdData\\abc.txt")
  dataframe1.show()

  val df2 = dataframe1.toDF("id", "name", "surname", "frinds", "grade", "email", "percentage")
  //Schema
  df2.printSchema()
  //show the data
  df2.show

  df2
    .groupBy("grade")
    .agg(
      count("*").as("groupCount"),
      max("percentage").as("sum"),
    ).show()

  df2
    .withColumn("rank", rank().over(Window.partitionBy("grade").orderBy(col("percentage").desc)))
    .withColumn("dense_rank", dense_rank().over(Window.partitionBy("grade").orderBy(col("percentage").desc)))
    .withColumn("min", min(col("percentage")).over(Window.partitionBy("grade")))
    .withColumn("max", max(col("percentage")).over(Window.partitionBy("grade").orderBy(col("percentage").desc)))
    .withColumn("lead", lead("percentage", 1, "NO lead VALUE AVAILABLE").over(Window.partitionBy("grade").orderBy(col("percentage").desc)))
    .withColumn("lag", lag("percentage", 1, "NO lag VALUE AVAILABLE").over(Window.partitionBy("grade").orderBy(col("percentage").desc)))
    .withColumn("diff", col("percentage") - col("lead"))
    .withColumn("MINREQUIRED_PERCENTAGE", lit("60********"))
    .show()

  spark.catalog.listTables().show()
  df2.createOrReplaceTempView("my_table")
  spark.catalog.listTables().show()

  spark.sql(
    """select *,
      | rank() OVER (PARTITION BY grade ORDER BY percentage DESC) as rank ,
      | dense_rank() OVER (PARTITION BY grade ORDER BY percentage DESC) as dense_rank ,
      | min(percentage) OVER (PARTITION BY grade ORDER BY percentage DESC) as min ,
      | max(percentage) OVER (PARTITION BY grade ORDER BY percentage DESC) as max ,
      | lead(percentage) OVER (PARTITION BY grade ORDER BY percentage DESC) as lead ,
      | lag(percentage) OVER (PARTITION BY grade ORDER BY percentage DESC) as lag,
      | '60********' as newcolumn
      | from my_table""".stripMargin).show()

}
