package Learning

import org.apache.spark.sql.SparkSession

object HiveTest5_ {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    //System.setProperty("hadoop.home.dir", "C:\\winutils")

    //first things first, start a spark session
    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    spark.sql("create table newone1(id Int,name String) row format delimited fields terminated by ','");
    //WE ALREADY CREATED THE TABLE SO WE DON'T NEED TO DO THAT AGAIN.
    //COMMENT OUT THE LINE ABOVE (line 28) IF YOU WANT TO RUN THIS!
    spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE newone1")
    spark.sql("SELECT * FROM newone1").show()
  }

}
