package com.viswa.cloud

import org.apache.spark.sql.{SaveMode, SparkSession}

object CSVLoader extends App {

  //Spark session creation
  val spark = SparkSession.builder().master("local").appName("DE").getOrCreate()

  //loading json file
  val df =
    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\viswa\\DATA\\test*")

  //Show the df
  df.show()
  df.printSchema()

  df.write.mode("overwrite").parquet("D:\\viswa\\PARQUET_SRC")

  val df2 = spark
    .read
    .parquet("D:\\viswa\\PARQUET_SRC")

  df2.printSchema()
  df2.show()
  spark.stop()

  //src has sent you 6.5GB => parquet => 200MB
}






//  val myWonArray = Array("hyd", "bang", "1-95 /abc -1788 - chennai/", "chennai-india")
//
//
//  val myWonArray2 = Array("abc@gmail", "xyz@gmail")
//
//  myWonArray.foreach(x => {
//    println("sending email to " + x)
//    // email code
//    println("sending has been sent !!!")
//  })
//
//  //System.exit(1)
//
//  println(myWonArray.contains("chennai"))
//
//  myWonArray.filterNot(x => {
//    println(x)
//    if (x.contains("chennai")) {
//      println("its matched ")
//      println(x)
//      true
//    } else {
//      println("No matching ")
//      false
//    }
//  }).foreach(x => {
//    println("sending email to " + x)
//    // email code
//    println("sending has been sent !!!")
//  })
//  //  println("===================> isEmpty :: " + filteredArray.isEmpty)
//  println("===================> length :: " + filteredArray.length)
//  println("===================> values :: " + filteredArray.mkString(","))


//val myWonArray = Array("hyd", "bang", "chennai")
//  val myWonArray = Array.empty
//  println(" Array elements count ==> " + myWonArray.length)
//  //println(" Array elements count ==> " + myWonArray.head)
//  //println(" Array elements count ==> " + myWonArray.tail.mkString("Array(", ", ", ")"))
//
//  if (myWonArray.length > 0)
//    println("Hi your value is positive ")
//  else if (myWonArray.isEmpty)
//    println("Your value is Zero ")
//  else
//    println("Your value is Negative")

//}
