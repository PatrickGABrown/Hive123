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
    import spark.implicits._
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
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE Bev_BranchA")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE Bev_BranchB")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE Bev_BranchC")


    //CREATING TABLES FOR THE CONSUMER COUNTS:
    //spark.sql("create table ConsCountA(drink String, count int) row format delimited fields terminated by ','");
    //spark.sql("create table ConsCountB(drink String, count int) row format delimited fields terminated by ','");
    //spark.sql("create table ConsCountC(drink String, count int) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE ConsCountA")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE ConsCountB")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE ConsCountC")

    //spark.sql("select * from ConsCountA").show()

    //Bev_BranchA
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
    //spark.sql("SELECT * FROM Bev_BranchB").show()
    //spark.sql("SELECT COUNT(bDrink) FROM Bev_BranchB").show()
    //spark.sql("SELECT * FROM Bev_BranchB WHERE branch = 'Branch8' AND drink LIKE '%MOCHA%'").show()

    //Bev_BranchC
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

     */

    //QUESTION 2:
    //Use the Branch1count table made from question one and group by and order by to show all the
    //drinks with their consumer numbers instead of just the total sum.
    //Use a limit and a desc to limit it to only the top answer.
    //spark.sql("SELECT drink, SUM(count) FROM Branch1count GROUP BY drink ORDER BY SUM(count) DESC LIMIT 1").show()

    //QUESTION 3:
    /*
    spark.sql("CREATE TABLE IF NOT EXISTS Branch1810drink AS SELECT drink, branch FROM \n" +
      "(SELECT * FROM Bev_BranchA WHERE branch = 'Branch1'\n" +
      " OR branch = 'Branch8' OR branch = 'Branch10' UNION \n" +
      "SELECT * FROM Bev_BranchB WHERE branch = 'Branch1'\n" +
      " OR branch = 'Branch8' OR branch = 'Branch10' UNION \n" +
      "SELECT * FROM Bev_BranchC WHERE branch = 'Branch1'\n" +
      " OR branch = 'Branch8' OR branch = 'Branch10')" )
    spark.sql("SELECT * FROM Branch1810drink").show(100)

     */

    //spark.sql("CREATE TABLE IF NOT EXISTS AllBranchDrinks(drink String, branch String) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE AllBranchDrinks")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE AllBranchDrinks")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE AllBranchDrinks")
    /*
    spark.sql("SELECT DISTINCT(drink) FROM AllBranchDrinks WHERE drink IN\n" +
      " (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch4')\n" +
      " AND drink IN (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch7')").show()

     */

    //QUESTION 4
    //Create partition on scenario 3
    /*
    spark.sql("DROP TABLE IF EXISTS partitioned3Table")
    spark.sql("CREATE TABLE IF NOT EXISTS partitioned3Table AS SELECT drink, branch FROM \n" +
      "(SELECT * FROM Bev_BranchA WHERE branch = 'Branch1'\n" +
      " OR branch = 'Branch8' OR branch = 'Branch10' UNION \n" +
      "SELECT * FROM Bev_BranchB WHERE branch = 'Branch1'\n" +
      " OR branch = 'Branch8' OR branch = 'Branch10' UNION \n" +
      "SELECT * FROM Bev_BranchC WHERE branch = 'Branch1'\n" +
      " OR branch = 'Branch8' OR branch = 'Branch10') PARTITIONED BY (branch)" )
    spark.sql("SELECT * FROM partitioned3Table").show()
     */

    //Create View on scenario 3
    /*
    spark.sql("CREATE VIEW Branch4and7DrinksView AS SELECT DISTINCT(drink) FROM AllBranchDrinks WHERE drink IN\n" +
      " (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch4')\n" +
      " AND drink IN (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch7')")

     */
    //spark.sql("SELECT * FROM Branch4and7DrinksView").show()

    //QUESTION 5:
    //spark.sql("ALTER TABLE Bev_BranchA SET tblproperties('notes' = 'These are the drinks offered in each branch in the A group')")
    //spark.sql("ALTER TABLE Bev_BranchB SET tblproperties('notes' = 'These are the drinks offered in each branch in the B group')")
    //spark.sql("ALTER TABLE Bev_BranchC SET tblproperties('notes' = 'These are the drinks offered in each branch in the C group')")
    //spark.sql("ALTER TABLE ConsCountA SET tblproperties('notes' = 'This is the consumer count for group A')")
    //spark.sql("ALTER TABLE ConsCountB SET tblproperties('notes' = 'This is the consumer count for group B')")
    //spark.sql("ALTER TABLE ConsCountB SET tblproperties('notes' = 'This is the consumer count for group C')")

    //spark.sql("ALTER TABLE Bev_BranchA SET tblproperties('comments' = 'This is a comment.')")
    //spark.sql("ALTER TABLE Bev_BranchB SET tblproperties('comments' = '')")
    //spark.sql("ALTER TABLE Bev_BranchC SET tblproperties('comments' = '')")
    //spark.sql("ALTER TABLE ConsCountA SET tblproperties('comments' = '')")
    //spark.sql("ALTER TABLE ConsCountB SET tblproperties('comments' = '')")
    //spark.sql("ALTER TABLE ConsCountC SET tblproperties('comments' = '')")

    //spark.sql("show tblproperties Bev_BranchA").show()
    //spark.sql("show tblproperties Bev_BranchB").show()
    //spark.sql("show tblproperties Bev_BranchC").show()
    //spark.sql("show tblproperties ConsCountA").show()
    //spark.sql("show tblproperties ConsCountB").show()
    //spark.sql("show tblproperties ConsCountC").show()


    /*
    //Trying to convert this all to Data Frames:
    //val path = "input/Bev_BranchA.txt"
    //val df1 = spark.read.textFile(path)
    //df1.show()
    case class Branch(drink: String, branch: String)
    val branchDF = spark.sparkContext.textFile("input/Bev_BranchA.txt")
    val d = branchDF.map(_.split(","))
    val branchAdf = d.map(x => Branch(x(0), x(1))).toDF()
    branchAdf.show(2)

     */

    //spark.sql("SELECT * FROM Branch4and7DrinksView ORDER BY drink DESC LIMIT 2").show()
    //spark.sql("DELETE FROM Branch4and7DrinksView WHERE drink IN (SELECT * FROM Branch4and7DrinksView WHERE drink = 'Triple_cappuccino' ORDER BY drink DESC LIMIT 1")
    //spark.sql("SELECT * FROM Branch4and7DrinksView ORDER BY drink DESC LIMIT 2").show()
    spark.sql("SELECT * FROM Branch4and7View").show()

    //QUESTION 6
    //FUTURE QUERY:







    //spark.sql("DROP TABLE IF EXISTS Branch1drink")
    //spark.sql("DROP TABLE IF EXISTS Branch1count")
    //spark.sql("DROP TABLE IF EXISTS Branch2drink")
    //spark.sql("DROP TABlE IF EXISTS Branch2count")
    //spark.sql("DROP TABLE IF EXISTS Branch4drink")
    //spark.sql("DROP TABlE IF EXISTS Branch7drink")
    //spark.sql("DROP TABLE IF EXISTS AllBranchDrinks")



    //CODE TO REMOVE MY TABLE TO START OVER:
    //spark.sql("DROP TABLE IF EXISTS Bev_BranchA")
    //spark.sql("DROP TABLE IF EXISTS Bev_BranchB")
    //spark.sql("DROP TABLE IF EXISTS Bev_BranchC")
  }

}

