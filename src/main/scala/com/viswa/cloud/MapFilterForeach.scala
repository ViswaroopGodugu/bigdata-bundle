package com.viswa.cloud

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

object MapFilterForeach {
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

    val schema = StructType(
      Array(
        StructField("DATE", StringType, nullable = true),
        StructField("ACC_NUM", IntegerType, nullable = true),
        StructField("CCY", StringType, nullable = true),
        StructField("AMOUNT", DoubleType, nullable = true),
        StructField("Mutilply", StringType, nullable = true),
        StructField("newCol", StringType, nullable = true),
        StructField("_corrupt_record", StringType, nullable = true),
        //StructField("__corrupt_record", StringType, nullable = true)
      )
    )
    val df = spark
      .read
      .option("header", value = true)
      .schema(schema)
      //.option("mode","FAILFAST")
      //.option("mode", "DROPMALFORMED")
      .option("mode", "PERMISSIVE") //Permissive
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      //.csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\Trans.csv.txt")
      .csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\events.txt")
      .withColumn("filename",input_file_name())

    //"D:\Batch1\CurrecnyExchange\Transcations\events.txt"

    df.show(false)
    df.printSchema()
//    df.where(expr("_corrupt_record is null")).show()
//    val numOfMalformedRows = df.where(col("_corrupt_record").isNull).count()
//    println(s"========>>> we have received ${numOfMalformedRows} in the Given file ")

  }
}

//val schema = StructType(
//      Array(
//        StructField("DATE", StringType, true),
//        StructField("ACC_NUM", IntegerType, true),
//        StructField("CCY", StringType, true),
//        StructField("AMOUNT", DoubleType, true),
//        StructField("Mutilply", StringType, true),
//        StructField("extra", StringType, true)
//      )
//    )
//    val df = spark
//      .read
//      .option("header", true)
//      .schema(schema)
//      .csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\Trans.csv.txt")
//
//
//    val schema2 = StructType(
//      Array(
//        StructField("ACC_NUM", IntegerType, true),
//        StructField("Description", StringType, true)
//      )
//    )
//
//    val df2= spark
//      .read
//      .option("header", true)
//      .schema(schema2)
//      //.csv("D:\\Batch1\\CurrecnyExchange\\Transcations10\\Trans.csv.txt")
//      .csv("D:\\Batch1\\CurrecnyExchange\\descriptions")
//
//   // val df2 = spark.emptyDataFrame
//
//    df2.show()
//    df.show
//
//    val df3 =
//      df.as("a")
//        .join(
//          df2.as("b"),
//          Seq("ACC_NUM"),
//          "left"
//        ).selectExpr("a.*", "b.Description as Desc")
//
//    df3.show()