package com.viswa.cloud

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, year}

object BasicSparkOps {
  var b: Int = _
  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder()
        .master("local[*]") //YARN
        .config("spark.app.name", "team_DE")
        .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    val dataframe1 = spark.read.option("header", "true").csv("D:\\Batch1\\test.csv")

    //Schema
    dataframe1.printSchema()

    //column schema converting
    val df3 = dataframe1.withColumn("yearnumber", col("Year").cast("Int"))

    //get partitions
    println("================> " + dataframe1.rdd.getNumPartitions)


    val df5 = dataframe1.repartition(20)
    println("================> " + df5.rdd.getNumPartitions)

    //Sql features

    //adding extra columns
    val df2 = dataframe1.withColumn("grpdata", col("Industry_code_NZSIOC"))


    //filtering data
    dataframe1.where("Variable_code like 'H01'").show()
    dataframe1.filter("Variable_code not like 'H01'").show()

    //aggs
    df3.show()
    df3.groupBy("yearnumber").count().show()
    df3.withColumn("newvalue", col("Value").cast("Int")).printSchema()
    df3.withColumn("newvalue", col("Value").cast("Int")).show()

    //create new column with Constant values
    df3.withColumn("constsntval", lit(1)).show()

    spark.catalog.listTables().show
    df3.createOrReplaceTempView("teamp_table")
    spark.catalog.listTables().show
    println("===================================================================")

    spark.sql("select * from teamp_table limit 4").show

    //Very Big data set
    // More rows filter  => filter
    // less data as ouput => where

    // dataframe1.show()

    //    val rdd = spark.sparkContext.parallelize(
    //      Seq(
    //        ("Java", 20000),
    //        ("Python", 100000),
    //        ("Scala", 3000)
    //      ), 2)
    //    val rdd2 = rdd.map(x => (x._1))
    //
    //    rdd2.foreach(x => {
    //      println(x)
    //      println("======>" + x + "   -- ")
    //    })
    //
    //    println("===============>   " + rdd.getStorageLevel)
    //    println("===============>   " + rdd.getNumPartitions)
    //
    //    val rdd4 = rdd.coalesce(1)
    //
    //    println("===============>  RDD4 " + rdd4.getStorageLevel)
    //    println("===============>  RDD4  " + rdd4.getNumPartitions)
    //
    //    val rddNext = spark.sparkContext.textFile("D:\\projects\\DATA\\sample.txt")
    //    rddNext.foreach(x => {
    //      println(x)
    //    })
    //    println("===============>  RDD4 " + rddNext.getStorageLevel)
    //    println("===============>  RDD4  " + rddNext.getNumPartitions)


    Thread.sleep(100000000)


    //
    //    spark.stop()
  }
}
