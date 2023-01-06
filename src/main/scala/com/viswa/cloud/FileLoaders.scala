package com.viswa.cloud

import org.apache.spark.sql.SparkSession

object FileLoaders {
  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("first_spark_program")
        .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    val df =
      spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("D:\\Batch1\\CurrecnyExchange\\Transcations\\Trans2.txt")
    df.show

    spark.catalog.listTables.show()
    df.createOrReplaceTempView("first_copy_view")
    spark.catalog.listTables.show()

    println("+" * 100)

    //    spark.table("first_copy_view").show()
    //    spark.sql("select * from first_copy_view").show()

    //ACC_NUM 12345
    df.where("ACC_NUM like '12345'").show()
    spark.sql(
      """
        |
        |select * from first_copy_view where ACC_NUM like '12345'
        |
        |
        |""".stripMargin).show()

    spark.sql("select concat('a','abc') as concatColumn").show()

    println("+" * 100)
  }
}
