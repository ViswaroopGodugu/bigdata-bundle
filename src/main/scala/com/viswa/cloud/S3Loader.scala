package com.viswa.cloud

import org.apache.spark.sql.{DataFrame, SparkSession}

object S3Loader extends App {
  val spark =
    SparkSession
      .builder()
      .master("local[*]") //YARN
      .config("spark.app.name", "team_DE")
      //.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")


  //  spark.conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  // spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  //spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
  //spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
  spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
  //spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

  spark.conf.set("fs.s3a.access.key", "AKIAXPRPXIKHP43VUPVP")
  spark.conf.set("fs.s3a.secret.key", "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")
  spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

  println(spark)
  //returns DataFrame
  val df: DataFrame = spark.read.text("s3a://bigdata-spark-test/set1.txt")
  df.printSchema()

}
