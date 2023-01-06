package com.viswa.cloud

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.shaded.com.google.common.primitives.Bytes

import java.io.{BufferedInputStream, File, FileInputStream}
import java.net.URI
import scala.math.Ordered.orderingToOrdered

object HDFSClient extends App {

  val fileSystem = FileSystem.get(new Configuration())

  // Check if the file already exists
  val path = new Path("hdfs://clutsername//abc.txt")

  if (fileSystem.exists(path)) {
    println("Filed Already exists")
  }

  // Create a new file and write data to it.
  val out = fileSystem.create(path)
  val in = new BufferedInputStream(new FileInputStream(new File("D:\\local\\abc.txt")));
  var numBytes = 0;
  var b = new Array[Byte](1024)
  while ((numBytes = in.read(b)) > 0) {
    out.write(b, 0, numBytes);
  }
  // Close all the file descriptors
  in.close()
  out.close()
  fileSystem.close()

  fileSystem.delete(new Path("hdfs://clutsername//abc.txt"))
  fileSystem.rename(new Path("hdfs://clutsername//abc.txt"),new Path("hdfs://clutsername//salesreport.txt"))

}
