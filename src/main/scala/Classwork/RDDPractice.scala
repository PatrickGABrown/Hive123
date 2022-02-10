package Classwork
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object RDDPractice {
  def main(args: Array[String]): Unit = {
    //first things first, start a spark session
    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    val rdd1 = spark.sparkContext.parallelize(Seq(1, 2, 3))
    rdd1.collect()
  }




}
