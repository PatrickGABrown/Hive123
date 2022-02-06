package Project1

import org.apache.spark.sql.SparkSession

object project1 {
  def main(args: Array[String]): Unit = {

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

    //WE ALREADY CREATED THE TABLE SO WE DON'T NEED TO DO THAT AGAIN.
    //COMMENT OUT THE CREATE LINES IF YOU WANT TO RUN THIS!
    //spark.sql("create table Bev_BranchA(drink String, branch String) row format delimited fields terminated by ','");

    //spark.sql("create table Bev_BranchB(drink String, branch String) row format delimited fields terminated by ','");

    //spark.sql("create table Bev_BranchC(drink String, branch String) row format delimited fields terminated by ','");

    //CREATING TABLES FOR THE CONSUMER COUNTS:
    //spark.sql("create table ConsCountA(drink String, count int) row format delimited fields terminated by ','");
    //spark.sql("create table ConsCountB(drink String, count int) row format delimited fields terminated by ','");
    //spark.sql("create table ConsCountC(drink String, count int) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE ConsCountA")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE ConsCountB")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE ConsCountC")
    //spark.sql("select * from ConsCountA").show()

    //Bev_BranchA
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE Bev_BranchA")
    //spark.sql("SELECT * FROM Bev_BranchA").show()
    //spark.sql("SELECT DISTINCT aDrink FROM Bev_BranchA").show()
    //spark.sql("SELECT * FROM Bev_BranchA WHERE aBranch = 'Branch9'").show()
    //spark.sql("SELECT * FROM Bev_BranchA WHERE aBranch = 'Branch1' AND aDrink = 'SMALL_Lite'").show()

    //THIS SOLVES SCENARIO3.2 FOR BRANCH A
    //HOW DO I COMBINE THIS FOR 3 DIFF TABLES?????
    /*
    spark.sql("SELECT DISTINCT(aDrink) FROM Bev_BranchA WHERE aDrink IN\n" +
      " (SELECT aDrink FROM Bev_BranchA WHERE aBranch = 'Branch6')\n" +
      " AND aDrink IN (SELECT aDrink FROM Bev_BranchA WHERE aBranch = 'Branch9')").show()

     */

    //spark.sql("SELECT aDrink FROM Bev_BranchA WHERE aBranch = 'Branch6'" +
    //" UNION SELECT aDrink FROM Bev_BranchA WHERE aBranch = 'Branch9'").show()
    //spark.sql("SELECT * FROM Bev_BranchA WHERE aBranch = 'Branch9' AND aDrink = 'Special_Lite'").show()

    //Bev_BranchB
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE Bev_BranchB")
    //spark.sql("SELECT * FROM Bev_BranchB").show()
    //spark.sql("SELECT COUNT(bDrink) FROM Bev_BranchB").show()
    //spark.sql("SELECT * FROM Bev_BranchB WHERE bBranch = 'Branch8' AND bDrink = 'SMALL_Lite'").show()

    //Bev_BranchC
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE Bev_BranchC")
    //spark.sql("SELECT * FROM Bev_BranchC").show()
    //spark.sql("SELECT * FROM Bev_BranchC WHERE cBranch = 'Branch2'").show()
    //spark.sql("SELECT * FROM Bev_BranchC WHERE cBranch = 'Branch9' AND cDrink = 'SMALL_Lite'").show()

    //GETTING THIS TO WORK:
    //QUESTION 1:
    /*
    //All beverages associated with branch1
    spark.sql("CREATE TABLE IF NOT EXISTS Branch1drink AS SELECT * FROM Bev_BranchA WHERE branch = 'Branch1'")
    spark.sql("INSERT INTO TABLE Branch1drink SELECT * FROM Bev_BranchB WHERE branch = 'Branch1'")
    spark.sql("INSERT INTO TABLE Branch1drink SELECT * FROM Bev_BranchC WHERE branch = 'Branch1'")
    //Create table with the counts of ALL the drinks in Branch 1
    spark.sql("CREATE TABLE IF NOT EXISTS Branch1count (drink string, count int)")
    spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountA.drink, SUM(ConsCountA.count) FROM ConsCountA\n" +
      " INNER JOIN Branch1drink ON (Branch1drink.drink = ConsCountA.drink) GROUP BY ConsCountA.drink")
    spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountB.drink, SUM(ConsCountB.count) FROM Branch1drink\n" +
      " INNER JOIN ConsCountB ON (Branch1drink.drink = ConsCountB.drink) GROUP BY ConsCountB.drink")
    spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountC.drink, SUM(ConsCountC.count) FROM Branch1drink\n" +
      " INNER JOIN ConsCountC ON (Branch1drink.drink = ConsCountC.drink) GROUP BY ConsCountC.drink")
    //Get the sum of all the counts for Branch 1
    spark.sql("SELECT SUM(count) FROM Branch1count").show()

     */

    //Select all the beverages in Branch 2
    spark.sql("CREATE TABLE IF NOT EXISTS Branch2drink AS SELECT * FROM Bev_BranchA WHERE branch = 'Branch2'")
    spark.sql("INSERT INTO TABLE Branch2drink SELECT * FROM Bev_BranchB WHERE branch = 'Branch2'")
    spark.sql("INSERT INTO TABLE Branch2drink SELECT * FROM Bev_BranchC WHERE branch = 'Branch3'")
    //Create a table with the counts of ALL the drinks in Branch 2
    spark.sql("CREATE TABLE IF NOT EXISTS Branch2count (drink string, count int)")
    //Add the sum of all drinks in the consumer count files A - B
    //Join that to the branch 2 drinks so they are associated with their consumer counts
    spark.sql("INSERT INTO TABLE Branch2count SELECT ConsCountA.drink, SUM(ConsCountA.count) FROM ConsCountA\n" +
      " INNER JOIN Branch2drink ON (Branch2drink.drink = ConsCountA.drink) GROUP BY ConsCountA.drink")
    spark.sql("INSERT INTO TABLE Branch2count SELECT ConsCountB.drink, SUM(ConsCountB.count) FROM Branch2drink\n" +
      " INNER JOIN ConsCountB ON (Branch2drink.drink = ConsCountB.drink) GROUP BY ConsCountB.drink")
    spark.sql("INSERT INTO TABLE Branch2count SELECT ConsCountC.drink, SUM(ConsCountC.count) FROM Branch2drink\n" +
      " INNER JOIN ConsCountC ON (Branch2drink.drink = ConsCountC.drink) GROUP BY ConsCountC.drink")
    //Get the sum of all the counts for Branch 2
    spark.sql("SELECT SUM(count) FROM Branch2count").show()




    //spark.sql("DROP TABLE Branch1drink")
    //spark.sql("DROP TABLE Branch1count")



    //CODE TO REMOVE MY TABLE TO START OVER:
    //spark.sql("DROP TABLE IF EXISTS Bev_BranchA")
    //spark.sql("DROP TABLE IF EXISTS Bev_BranchB")
    //spark.sql("DROP TABLE IF EXISTS Bev_BranchC")
  }

}

