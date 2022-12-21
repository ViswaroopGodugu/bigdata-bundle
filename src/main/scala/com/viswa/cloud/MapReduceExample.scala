package com.viswa.cloud

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

// This class performs the map operation, translating raw input into the key-value
// pairs we will feed into our reduce operation.
//hdfs://clusterid/user/abc.txt
//DATE,ACC_NUM,CCY,AMOUNT,Mutilply
//20221215,12345,INR,500,aa
//20221214,8682,USD,70,vv
//20221214,1239,USD,990,vv
//20221214,1239,USD,990,vv
//20221214,1239,USD,990,vv
class TokenizerMapper extends Mapper[
  Object, Text, //input classes
  Text, IntWritable //output
] {
  val one = new IntWritable(1)
  val word = new Text

  override
  def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) = {
    val eachRowItams = value.toString().split(",")
    for (t <- eachRowItams) {
      word.set(t)
      context.write(word, one)
    }
  }
}

// This class performs the reduce operation, iterating over the key-value pairs
// produced by our map operation to produce a result. In this case we just
// calculate a simple total for each word seen.
class IntSumReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  override
  def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context) = {
    val sum = values.foldLeft(0) { (t, i) => t + i.get }
    context.write(key, new IntWritable(sum))
  }
}

// This class configures and runs the job with the map and reduce classes we've
// specified above.
object MapReduceExample {

  def main(args: Array[String]): Int = {
    val conf = new Configuration()

    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("Usage: word count <in> <out>")
      return 2
    }

    val job = new Job(conf, "word count")

    job.setJarByClass(classOf[TokenizerMapper])

    job.setMapperClass(classOf[TokenizerMapper])

    job.setCombinerClass(classOf[IntSumReducer])

    job.setReducerClass(classOf[IntSumReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0))) //hdfs://clusterid/username/input/abc.txt
    FileOutputFormat.setOutputPath(job, new Path((args(1)))) //hdfs://clusterid/username/output/part0-fghjkkfghjkl.txt
    //job.submit()
    if (job.waitForCompletion(true)) 0 else 1
  }
}

