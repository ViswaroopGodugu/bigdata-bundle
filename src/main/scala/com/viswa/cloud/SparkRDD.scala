package com.viswa.cloud

import org.apache.spark.sql.SparkSession
case class Example(DATE: String, ACC_NUM: String, CCY: String, AMOUNT: String, Mutilply: String){}

object SparkRDD {
  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("first_spark_program")
        .getOrCreate()

    val sC = spark.sparkContext
    spark.sparkContext.setLogLevel("OFF")
    println("Spark Session created Successfully  !! " + spark)

    val dataSeq =
      Seq(
        ("abc", 100),
        ("xyz", 100),
        ("xyz", 200),
        ("xyz", 200)
      )
    val rdd = sC.parallelize(dataSeq, 2)
    rdd.foreach(x => println(x._1 + " ---> " + x._2))
    println("----- Num of partitions  " + rdd.getNumPartitions)

    val rddFile =
      sC.textFile("D:\\Batch1\\CurrecnyExchange\\Transcations\\Trans2.txt", 2)
    rddFile.foreach(x => println(x))
    println("----- Num of partitions  " + rddFile.getNumPartitions)

    val df =
      spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\Trans2.txt")
    df.show


    import spark.implicits._
    val ds = df.as[Example]

    //  Compile time safety
    print(rdd.first()._1)

    println(ds.first().Mutilply)
    println(df.first().getAs("Mutilply"))

  }
}
