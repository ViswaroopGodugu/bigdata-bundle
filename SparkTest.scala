Import org.apache.spark._
object SparkTest extends App{

Val spark = SparkSession.builder().getOrCrete()

Spark.conf.forEach(prob => println(s"$prob.key ==> $prob.value")

Spark.stop

}
