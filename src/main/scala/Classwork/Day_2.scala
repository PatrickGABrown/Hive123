package Classwork

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day_2 {
  def main(args: Array[String]): Unit ={
    //first things first, start a spark session
    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    //QUESTION 1
    val df = spark.read.option("multiline", "true").json("input/bank_edited.json")
    //df.show()
    df.createOrReplaceTempView("day2")
    //spark.sql("select * from day2").show()

    //QUESTION 2
    //spark.sql("select (count(y) from day2 where y='no')/(select count(*) from day2) as result from day2").show()
    //spark.sql("select round((select count(y) from day2 where y = 'no')/count(*), 2)*100 as failPercent from day2").show()

    //QUESTION 3
    //MAX AGE
    //spark.sql("select max(age) from day2").show()
    //MIN AGE
    //spark.sql("select min(age) from day2").show()
    //MEAN AGE
    //spark.sql("select round(sum(age)/count(age)) as meanAge from day2").show()
    //spark.sql("select round(avg(age)) from day2").show()

    //QUESTION 4
    //Check avg and median balance of customers
    //spark.sql("select avg(balance) from day2").show()
    //spark.sql("select count(balance) from day2").show()
    //spark.sql("select balance from (select ROW_NUMBER() OVER (ORDER BY key ASC) AS rownum from day2) as medBal where rownum = 45212/2")
    //spark.sql("select balance from day2 order by balance")
    //val avgBalance = df.select(avg("balance")).collect()(0)(0)
    //println("average balance is " + avgBalance)
    //val medianArray = df.stat.approxQuantile("balance", Array(0.5), 0)
    //println("median balance is " + medianArray(0))

    //QUESTION 5
    //spark.sql("select (select count(y) from day2 where y = 'yes') as yesCount, age from day2 group by age").show
    //spark.sql("select age, count(*) as yesCount from day2 where y = 'yes' group by age order by age").show()
    //spark.sql("select age, count(*) as noCount from day2 where y = 'no' group by age order by age").show()

    spark.sql("select day2.age, count(day2.y) as yes, t.no from day2 join \n" +
      "(select age, count(y) as no from day2 where y = 'no' group by age) as t on day2.age = t.age \n" +
      "where day2.y = 'yes' group by day2.age, t.no order by day2.age asc").show()

    //QUESTION 6
    //spark.sql("select count(select y from day2 where y = 'yes'), marital from day2 group by marital").show
    //spark.sql("select marital, count(*) as yesCounts from day2 where y = 'yes' group by marital").show()
    //spark.sql("select marital, count(*) as noCounts from day2 where y = 'no' group by marital").show()
    //println("\nQuestion 6:")
    //val temp = spark.sql("select day2.marital, count(day2.y) as no, t.total from day2 join (select marital, count(*) as total from day2 group by marital) as t on day2.marital = t.marital where day2.y = 'no' group by day2.marital, t.total")
    //temp.createOrReplaceTempView("q6");
    //spark.sql("select marital, round((no * 100.0 / total), 2) as percent_no from q6;").show();

    //QUESTION 7




  }


}
