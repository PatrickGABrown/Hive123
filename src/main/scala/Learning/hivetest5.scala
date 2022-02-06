package Learning

import org.apache.spark.sql.SparkSession

object hivetest5 {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    //System.setProperty("hadoop.home.dir", "C:\\winutils")

    //first things first, start a spark session
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    //Here we create a sequence RDD and set it to a variable called sampleSeq
    val sampleSeq = Seq((1, "spark"), (2, "Big Data"), (3, "scala"))

    //createDataFrame() will create a data frame from an RDD (it takes RDDs as an argument)
    //.toDF() turns the collection into a DF with specified columns.
    //So you can chain toDF() with createDataFrame() to specify column names.
    val df = spark.createDataFrame(sampleSeq).toDF("Course id", "course name")
    df.show()
    df.write.format("csv").save("sampleSeq")
  }


}
