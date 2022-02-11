//Patrick Brown
//2/11/22
//Project 1

package Project1

import Project1.MenuDetails._

import scala.io.StdIn.readLine
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.collection.immutable.ListMap


object Main {

  def main (args: Array[String]): Unit = {

    //first things first, start a spark session
    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    //spark.sql("set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict")

    //CREATING TABLES FOR BRANCH DRINKS

    spark.sql("DROP TABLE IF EXISTS Bev_BranchA")
    spark.sql("DROP TABLE IF EXISTS Bev_BranchB")
    spark.sql("DROP TABLE IF EXISTS Bev_BranchC")
    spark.sql("create table Bev_BranchA(drink String, branch String) row format delimited fields terminated by ','");
    spark.sql("create table Bev_BranchB(drink String, branch String) row format delimited fields terminated by ','");
    spark.sql("create table Bev_BranchC(drink String, branch String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE Bev_BranchA")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE Bev_BranchB")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE Bev_BranchC")


    //CREATING TABLES FOR THE CONSUMER COUNTS:
    spark.sql("DROP TABLE IF EXISTS ConsCountA")
    spark.sql("DROP TABLE IF EXISTS ConsCountB")
    spark.sql("DROP TABLE IF EXISTS ConsCountC")
    spark.sql("create table ConsCountA(drink String, count int) row format delimited fields terminated by ','");
    spark.sql("create table ConsCountB(drink String, count int) row format delimited fields terminated by ','");
    spark.sql("create table ConsCountC(drink String, count int) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE ConsCountA")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE ConsCountB")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE ConsCountC")



    //QUESTION 1 METHOD
    def p1func(): Unit = {
      println("QUESTION 1 ANSWERS:")
      //All beverages associated with branch1
      spark.sql("DROP TABLE IF EXISTS Branch1drink")
      spark.sql("CREATE TABLE IF NOT EXISTS Branch1drink AS SELECT * FROM Bev_BranchA WHERE branch = 'Branch1'")
      spark.sql("INSERT INTO TABLE Branch1drink SELECT * FROM Bev_BranchB WHERE branch = 'Branch1'")
      spark.sql("INSERT INTO TABLE Branch1drink SELECT * FROM Bev_BranchC WHERE branch = 'Branch1'")
      //Create table with the counts of ALL the drinks in Branch 1
      spark.sql("DROP TABLE IF EXISTS Branch1count")
      spark.sql("CREATE TABLE IF NOT EXISTS Branch1count (drink string, count int)")
      spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountA.drink, SUM(ConsCountA.count) FROM ConsCountA\n" +
        " INNER JOIN Branch1drink ON (Branch1drink.drink = ConsCountA.drink) GROUP BY ConsCountA.drink")
      spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountB.drink, SUM(ConsCountB.count) FROM Branch1drink\n" +
        " INNER JOIN ConsCountB ON (Branch1drink.drink = ConsCountB.drink) GROUP BY ConsCountB.drink")
      spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountC.drink, SUM(ConsCountC.count) FROM Branch1drink\n" +
        " INNER JOIN ConsCountC ON (Branch1drink.drink = ConsCountC.drink) GROUP BY ConsCountC.drink")
      //Get the sum of all the counts for Branch 1
      println("The total number of consumers for branch 1 is ")
      spark.sql("SELECT SUM(count) FROM Branch1count").show()


      //Select all the beverages in Branch 2
      spark.sql("DROP TABLE IF EXISTS Branch2drink")
      spark.sql("CREATE TABLE IF NOT EXISTS Branch2drink AS SELECT * FROM Bev_BranchA WHERE branch = 'Branch2'")
      spark.sql("INSERT INTO TABLE Branch2drink SELECT * FROM Bev_BranchB WHERE branch = 'Branch2'")
      spark.sql("INSERT INTO TABLE Branch2drink SELECT * FROM Bev_BranchC WHERE branch = 'Branch2'")
      //Create a table with the counts of ALL the drinks in Branch 2
      spark.sql("DROP TABLE IF EXISTS Branch2count")
      spark.sql("CREATE TABLE IF NOT EXISTS Branch2count (drink string, count int)")
      //Add the sum of all drinks in the consumer count files A - B
      //Join that to the branch 2 drinks so they are associated with their consumer counts
      spark.sql("INSERT INTO TABLE Branch2count SELECT ConsCountA.drink, SUM(ConsCountA.count) FROM Branch2drink\n" +
        " INNER JOIN ConsCountA ON (Branch2drink.drink = ConsCountA.drink) GROUP BY ConsCountA.drink")
      spark.sql("INSERT INTO TABLE Branch2count SELECT ConsCountB.drink, SUM(ConsCountB.count) FROM Branch2drink\n" +
        " INNER JOIN ConsCountB ON (Branch2drink.drink = ConsCountB.drink) GROUP BY ConsCountB.drink")
      spark.sql("INSERT INTO TABLE Branch2count SELECT ConsCountC.drink, SUM(ConsCountC.count) FROM Branch2drink\n" +
        " INNER JOIN ConsCountC ON (Branch2drink.drink = ConsCountC.drink) GROUP BY ConsCountC.drink")
      //Get the sum of all the counts for Branch 2
      println("The total number of consumers for branch 2 is ")
      spark.sql("SELECT SUM(count) FROM Branch2count").show()
      //call method to start the app over
      startOver()
    }

    //QUESTION 2 METHOD:
    def p2func(): Unit = {
      println("QUESTION 2 ANSWERS:")
      //Count of all beverages associated with branch1
      spark.sql("DROP TABLE IF EXISTS Branch1count")
      spark.sql("CREATE TABLE IF NOT EXISTS Branch1count (drink string, count int)")
      spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountA.drink, SUM(ConsCountA.count) FROM ConsCountA\n" +
        " INNER JOIN Branch1drink ON (Branch1drink.drink = ConsCountA.drink) GROUP BY ConsCountA.drink")
      spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountB.drink, SUM(ConsCountB.count) FROM Branch1drink\n" +
        " INNER JOIN ConsCountB ON (Branch1drink.drink = ConsCountB.drink) GROUP BY ConsCountB.drink")
      spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountC.drink, SUM(ConsCountC.count) FROM Branch1drink\n" +
        " INNER JOIN ConsCountC ON (Branch1drink.drink = ConsCountC.drink) GROUP BY ConsCountC.drink")

      //Use the Branch1count table made from question one and group by and order by to show all the
      //drinks with their consumer numbers instead of just the total sum.
      //Use a limit and a desc to limit it to only the top answer.
      println("The most consumed beverage on Branch 1 is ")
      spark.sql("SELECT drink, SUM(count) FROM Branch1count GROUP BY drink ORDER BY SUM(count) DESC LIMIT 1").show()

      //Create a table with the counts of ALL the drinks in Branch 2
      spark.sql("DROP TABLE IF EXISTS Branch2count")
      spark.sql("CREATE TABLE IF NOT EXISTS Branch2count (drink string, count int)")
      spark.sql("INSERT INTO TABLE Branch2count SELECT ConsCountA.drink, SUM(ConsCountA.count) FROM ConsCountA\n" +
        " INNER JOIN Branch2drink ON (Branch2drink.drink = ConsCountA.drink) GROUP BY ConsCountA.drink")
      spark.sql("INSERT INTO TABLE Branch2count SELECT ConsCountB.drink, SUM(ConsCountB.count) FROM Branch2drink\n" +
        " INNER JOIN ConsCountB ON (Branch2drink.drink = ConsCountB.drink) GROUP BY ConsCountB.drink")
      spark.sql("INSERT INTO TABLE Branch2count SELECT ConsCountC.drink, SUM(ConsCountC.count) FROM Branch2drink\n" +
        " INNER JOIN ConsCountC ON (Branch2drink.drink = ConsCountC.drink) GROUP BY ConsCountC.drink")

      //Use the Branch2count table made from question one and group by and order by to show all the
      //drinks with their consumer numbers instead of just the total sum.
      //Use a limit to limit it to only the top answer which should be the least consumed.
      println("The least consumed beverage on Branch 2 is ")
      spark.sql("SELECT drink, SUM(count) FROM Branch2count GROUP BY drink ORDER BY SUM(count) LIMIT 1").show()

      //AVERAGE CONSUMED BEVERAGE FOR A BRANCH
      //NOT WORKING!
      //println("The average consumed beverage from Branch 2 is ")
      //spark.sql("SELECT drink, AVG(count) FROM Branch2count GROUP BY drink").show()
      println("The average number of beverages consumed from Branch 2 is ")
      spark.sql("SELECT AVG(count) FROM Branch2count").show()
      //call method to start the app over
      startOver()
    }

    //QUESTION 3 METHOD:
    def p3func(): Unit = {
      println("QUESTION 3 SOLUTION:")
      spark.sql("DROP TABLE IF EXISTS Branch1810drink")
      spark.sql("CREATE TABLE IF NOT EXISTS Branch1810drink AS SELECT drink, branch FROM \n" +
        "(SELECT * FROM Bev_BranchA WHERE branch = 'Branch1'\n" +
        " OR branch = 'Branch8' OR branch = 'Branch10' UNION \n" +
        "SELECT * FROM Bev_BranchB WHERE branch = 'Branch1'\n" +
        " OR branch = 'Branch8' OR branch = 'Branch10' UNION \n" +
        "SELECT * FROM Bev_BranchC WHERE branch = 'Branch1'\n" +
        " OR branch = 'Branch8' OR branch = 'Branch10')" )
      println("The beverages available in Branches 1, 8, and 10 are ")
      spark.sql("SELECT * FROM Branch1810drink").show(100)

      spark.sql("DROP TABLE IF EXISTS AllBranchDrinks")
      spark.sql("CREATE TABLE IF NOT EXISTS AllBranchDrinks(drink String, branch String) row format delimited fields terminated by ','")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE AllBranchDrinks")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE AllBranchDrinks")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE AllBranchDrinks")
      println("The common beverages available in Branches 4 and 7 are ")
      spark.sql("SELECT DISTINCT(drink) FROM AllBranchDrinks WHERE drink IN\n" +
        " (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch4')\n" +
        " AND drink IN (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch7')").show(100)
      //Call method to start the app over
      startOver()
    }

    //QUESTION 4 METHOD:
    def p4func(): Unit = {
      println("QUESTION 4 ANSWERS:")

      //Create partition on scenario 3
      spark.sql("DROP TABLE IF EXISTS AllBranchDrinks")
      spark.sql("CREATE TABLE IF NOT EXISTS AllBranchDrinks(drink String, branch String) row format delimited fields terminated by ','")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE AllBranchDrinks")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE AllBranchDrinks")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE AllBranchDrinks")
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      //spark.sql("SELECT * FROM Bev_BranchA").show()
      spark.sql("CREATE TABLE IF NOT EXISTS partBranch(drink String) PARTITIONED BY (branch String)")
      spark.sql("INSERT OVERWRITE TABLE partBranch PARTITION (branch) SELECT drink, branch FROM AllBranchDrinks")
      println("The table AllBranchDrinks is now partitioned on branch column")
      spark.sql("SELECT * FROM partBranch").show()

      println("Here is information about the partitioned table:")
      spark.sql("DESCRIBE FORMATTED partBranch").show()

      //Create View on scenario 3
      spark.sql("DROP VIEW IF EXISTS Branch4and7DrinksView")
      spark.sql("CREATE VIEW IF NOT EXISTS Branch4and7DrinksView AS SELECT DISTINCT(drink) FROM AllBranchDrinks WHERE drink IN\n" +
        " (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch4')\n" +
        " AND drink IN (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch7')")
      println("This is the view for part 2 of question 3")
      spark.sql("SELECT * FROM Branch4and7DrinksView").show()

      //Calls method to start app over.
      startOver()
    }

    //QUESTION 5 METHOD:
    def p5func(): Unit = {
      println("QUESTION 5 ANSWERS:")
      //CREATING NOTES
      spark.sql("ALTER TABLE Bev_BranchA SET tblproperties('notes' = 'These are the drinks offered in each branch in the A group')")
      //spark.sql("ALTER TABLE Bev_BranchB SET tblproperties('notes' = 'These are the drinks offered in each branch in the B group')")
      //spark.sql("ALTER TABLE Bev_BranchC SET tblproperties('notes' = 'These are the drinks offered in each branch in the C group')")
      //spark.sql("ALTER TABLE ConsCountA SET tblproperties('notes' = 'This is the consumer count for group A')")
      //spark.sql("ALTER TABLE ConsCountB SET tblproperties('notes' = 'This is the consumer count for group B')")
      //spark.sql("ALTER TABLE ConsCountB SET tblproperties('notes' = 'This is the consumer count for group C')")

      //CREATING COMMENTS
      spark.sql("ALTER TABLE Bev_BranchA SET tblproperties('comments' = 'This is a comment.')")
      //spark.sql("ALTER TABLE Bev_BranchB SET tblproperties('comments' = '')")
      //spark.sql("ALTER TABLE Bev_BranchC SET tblproperties('comments' = '')")
      //spark.sql("ALTER TABLE ConsCountA SET tblproperties('comments' = '')")
      //spark.sql("ALTER TABLE ConsCountB SET tblproperties('comments' = '')")
      //spark.sql("ALTER TABLE ConsCountC SET tblproperties('comments' = '')")

      //DISPLAY TABLE PROPERTIES
      println("New table properties added to Bev_BranchA table: ")
      spark.sql("show tblproperties Bev_BranchA").show()
      //spark.sql("show tblproperties Bev_BranchB").show()
      //spark.sql("show tblproperties Bev_BranchC").show()
      //spark.sql("show tblproperties ConsCountA").show()
      //spark.sql("show tblproperties ConsCountB").show()
      //spark.sql("show tblproperties ConsCountC").show()

      //Remove a row from a table
      //Recreate the view from scenario 3
      spark.sql("DROP TABLE IF EXISTS AllBranchDrinks")
      spark.sql("CREATE TABLE IF NOT EXISTS AllBranchDrinks(drink String, branch String) row format delimited fields terminated by ','")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE AllBranchDrinks")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE AllBranchDrinks")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE AllBranchDrinks")
      spark.sql("DROP VIEW IF EXISTS Branch4and7DrinksView")
      spark.sql("CREATE VIEW IF NOT EXISTS Branch4and7DrinksView AS SELECT DISTINCT(drink) FROM AllBranchDrinks WHERE drink IN\n" +
        " (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch4')\n" +
        " AND drink IN (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch7')")
      println("This is the table before delete.")
      spark.sql("select * from Branch4and7DrinksView ORDER BY drink DESC").show()
      println("This is the table after delete.")
      //Uses left join to remove an item from the view in scenario 3
      /*
      spark.sql("SELECT * FROM Branch4and7DrinksView A LEFT JOIN \n" +
      "(SELECT * FROM Branch4and7DrinksView WHERE drink = 'Triple_cappuccino' ORDER BY drink DESC LIMIT 1) B ON \n +" +
        "A.drink = B.drink WHERE B.drink IS NULL ORDER BY A.drink DESC").show()

       */
      spark.sql("SELECT * FROM Branch4and7DrinksView A WHERE A.drink NOT IN  \n" +
        "(SELECT * FROM Branch4and7DrinksView WHERE drink = 'Triple_cappuccino' ORDER BY drink DESC LIMIT 1) \n" +
      "ORDER BY A.drink DESC").show()
      //spark.sql("SELECT * FROM Branch4and7DrinksView WHERE drink = 'Triple_cappuccino' ORDER BY drink DESC LIMIT 1").show()
      //spark.sql("select * from Branch4and7DrinksView ORDER BY drink DESC").show()

      //Call method to start app over:
      startOver()
    }

    //QUESTION 6 METHOD: FUTURE QUERY:
    def p6func(): Unit = {
      println("FUTURE QUERY!")
      //basePrice TABLE WITH ALL THE PRICES FOR THE DRINKS
      spark.sql("DROP TABLE IF EXISTS basePrice")
      spark.sql("CREATE TABLE IF NOT EXISTS basePrice (drink String, price decimal(10, 2))")
      spark.sql("INSERT INTO basePrice VALUES ('Special_Lite', 4.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Special_LATTE', 4.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Special_cappuccino', 4.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Special_Espresso', 4.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Special_Coffee', 4.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Special_MOCHA', 4.00)")

      spark.sql("INSERT INTO basePrice VALUES ('SMALL_Lite', 1.00)")
      spark.sql("INSERT INTO basePrice VALUES ('SMALL_LATTE', 1.00)")
      spark.sql("INSERT INTO basePrice VALUES ('SMALL_cappuccino', 1.00)")
      spark.sql("INSERT INTO basePrice VALUES ('SMALL_Espresso', 1.00)")
      spark.sql("INSERT INTO basePrice VALUES ('SMALL_Coffee', 1.00)")
      spark.sql("INSERT INTO basePrice VALUES ('SMALL_MOCHA', 1.00)")

      spark.sql("INSERT INTO basePrice VALUES ('MED_Lite', 2.00)")
      spark.sql("INSERT INTO basePrice VALUES ('MED_LATTE', 2.00)")
      spark.sql("INSERT INTO basePrice VALUES ('MED_cappuccino', 2.00)")
      spark.sql("INSERT INTO basePrice VALUES ('MED_Espresso', 2.00)")
      spark.sql("INSERT INTO basePrice VALUES ('MED_Coffee', 2.00)")
      spark.sql("INSERT INTO basePrice VALUES ('MED_MOCHA', 2.00)")

      spark.sql("INSERT INTO basePrice VALUES ('LARGE_Lite', 3.00)")
      spark.sql("INSERT INTO basePrice VALUES ('LARGE_LATTE', 3.00)")
      spark.sql("INSERT INTO basePrice VALUES ('LARGE_cappuccino', 3.00)")
      spark.sql("INSERT INTO basePrice VALUES ('LARGE_Espresso', 3.00)")
      spark.sql("INSERT INTO basePrice VALUES ('LARGE_Coffee', 3.00)")
      spark.sql("INSERT INTO basePrice VALUES ('LARGE_MOCHA', 3.00)")

      spark.sql("INSERT INTO basePrice VALUES ('ICY_Lite', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('ICY_LATTE', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('ICY_cappuccino', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('ICY_Espresso', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('ICY_Coffee', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('ICY_MOCHA', 1.30)")

      spark.sql("INSERT INTO basePrice VALUES ('Triple_Lite', 3.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Triple_LATTE', 3.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Triple_cappuccino', 3.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Triple_Espresso', 3.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Triple_Coffee', 3.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Triple_MOCHA', 3.00)")

      spark.sql("INSERT INTO basePrice VALUES ('Mild_Lite', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('Mild_LATTE', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('Mild_cappuccino', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('Mild_Espresso', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('Mild_Coffee', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('Mild_MOCHA', 1.30)")

      spark.sql("INSERT INTO basePrice VALUES ('Double_Lite', 2.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Double_LATTE', 2.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Double_cappuccino', 2.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Double_Espresso', 2.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Double_Coffee', 2.00)")
      spark.sql("INSERT INTO basePrice VALUES ('Double_MOCHA', 2.00)")

      spark.sql("INSERT INTO basePrice VALUES ('Cold_Lite', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('Cold_LATTE', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('Cold_cappuccino', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('Cold_Espresso', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('Cold_Coffee', 1.30)")
      spark.sql("INSERT INTO basePrice VALUES ('Cold_MOCHA', 1.30)")
      //spark.sql("SELECT * FROM basePrice ORDER BY drink").show()

      //NEW TABLE OF ALL A DRINKS AND COUNTS
      spark.sql("DROP TABLE IF EXISTS AdrinkandCount")
      spark.sql("CREATE TABLE IF NOT EXISTS AdrinkandCount (drink string, branch String, count int)")
      spark.sql("INSERT INTO TABLE AdrinkandCount SELECT drink, branch, tc from \n" +
        "(select ConsCountA.drink, branch, sum(ConsCountA.count)tc from \n" +
        "(select drink, branch from (select * from Bev_BranchA UNION ALL select * from Bev_BranchB UNION ALL select * from Bev_BranchC)) X \n" +
        " JOIN ConsCountA ON (X.drink = ConsCountA.drink) GROUP BY ConsCountA.drink, X.branch)")
      //spark.sql("SELECT sum(count) FROM AdrinkandCount where branch = 'Branch2'").show()
      //spark.sql("ALTER TABLE AdrinkandCount ADD COLUMN price DECIMAL(10, 2) AFTER count")

      //CREATE TABLE FOR REVANUE OF EACH DRINK IN A
      spark.sql("DROP TABLE IF EXISTS aNew")
      //spark.sql("CREATE TABLE IF NOT EXISTS aNew(drink string, branch string, count int, ")
      spark.sql("CREATE TABLE IF NOT EXISTS aNew AS SELECT a.drink, a.branch, a.count, (x.price*a.count) as tRev\n" +
        "from AdrinkandCount a inner join \n" +
        "basePrice x on a.drink=x.drink order by a.drink")
      //spark.sql("SELECT sum(count) FROM aNew where branch = 'Branch1'").show()
      //spark.sql("select sum(count) from aNew").show()
      //spark.sql("select * from aNew where drink = 'Cold_Coffee' order by drink").show()

      //aRev has the max revenue for each drink in table A
      spark.sql("drop table if exists aRev")
      spark.sql("create table if not exists aRev as select sum(tRev) as rev, drink from aNew group by drink order by drink")
      //spark.sql("select * from cRev").show()
      //spark.sql("select max(d.sum), drink from (select sum(tRev), drink from cNew group by drink order by drink) d group by drink").show()
      //spark.sql("select drink, rev from aRev where rev in (select max(rev) from aRev)").show()


      //NEW TABLE FOR ALL DRINKS AND COUNTS FOR B
      spark.sql("DROP TABLE IF EXISTS BdrinkandCount")
      spark.sql("CREATE TABLE IF NOT EXISTS BdrinkandCount (drink string, branch String, count int)")
      spark.sql("INSERT INTO TABLE BdrinkandCount SELECT drink, branch, tc from \n" +
        "(select ConsCountB.drink, branch, sum(ConsCountB.count)tc from \n" +
        "(select drink, branch from (select * from Bev_BranchA UNION ALL select * from Bev_BranchB UNION ALL select * from Bev_BranchC)) X \n" +
        " JOIN ConsCountB ON (X.drink = ConsCountB.drink) GROUP BY ConsCountB.drink, X.branch)")
      //spark.sql("SELECT sum(count) FROM BdrinkandCount where branch = 'Branch1'").show()
      //spark.sql("select * from BdrinkandCount").show()
      spark.sql("ALTER TABLE BdrinkandCount ADD COLUMN price DECIMAL(10, 2) AFTER count")
      //spark.sql("select * from BdrinkandCount").show()

      //CREATE TABLE FOR REVANUE OF EACH DRINK IN B
      spark.sql("DROP TABLE IF EXISTS bNew")
      spark.sql("CREATE TABLE IF NOT EXISTS bNew AS SELECT b.drink, b.branch, b.count, (x.price*b.count) as tRev\n" +
        "from BdrinkandCount b inner join \n" +
        "basePrice x on b.drink=x.drink order by b.drink")
      //spark.sql("SELECT sum(count) FROM bNew").show()
      //spark.sql("select * from bNew where drink = 'Cold_Coffee' order by drink").show()

      //bRev has the max revenue for each drink in table B
      spark.sql("drop table if exists bRev")
      spark.sql("create table if not exists bRev as select sum(tRev) as rev, drink from bNew group by drink order by drink")
      //spark.sql("select * from cRev").show()
      //spark.sql("select max(d.sum), drink from (select sum(tRev), drink from cNew group by drink order by drink) d group by drink").show()
      //spark.sql("select drink, rev from bRev where rev in (select max(rev) from bRev)").show()


      //NEW TABLE FOR ALL DRINKS AND COUNTS FOR C
      spark.sql("DROP TABLE IF EXISTS CdrinkandCount")
      spark.sql("CREATE TABLE IF NOT EXISTS CdrinkandCount (drink string, branch String, count int)")
      spark.sql("INSERT INTO TABLE CdrinkandCount SELECT drink, branch, tc from \n" +
        "(select ConsCountC.drink, branch, sum(ConsCountC.count)tc from \n" +
        "(select drink, branch from (select * from Bev_BranchA UNION ALL select * from Bev_BranchB UNION ALL select * from Bev_BranchC)) X \n" +
        " JOIN ConsCountC ON (X.drink = ConsCountC.drink) GROUP BY ConsCountC.drink, X.branch)")
      //spark.sql("SELECT sum(count) FROM CdrinkandCount where branch = 'Branch1'").show()
      spark.sql("ALTER TABLE CdrinkandCount ADD COLUMN price DECIMAL(10, 2) AFTER count")
      //spark.sql("select * from CdrinkandCount").show()

      //CREATE TABLE FOR REVANUE OF EACH DRINK IN C
      spark.sql("DROP TABLE IF EXISTS cNew")
      spark.sql("CREATE TABLE IF NOT EXISTS cNew AS SELECT c.drink, c.branch, c.count, (x.price*c.count) as tRev\n" +
        "from CdrinkandCount c inner join \n" +
        "basePrice x on c.drink=x.drink order by c.drink")
      //spark.sql("SELECT * FROM cNew'").show()
      //spark.sql("SELECT sum(count) FROM cNew").show()

      //cRev has the max revenue for each drink in table C
      spark.sql("drop table if exists cRev")
      spark.sql("create table if not exists cRev as select sum(tRev) as rev, drink from cNew group by drink order by drink")
      //spark.sql("select * from cRev").show()
      //spark.sql("select max(d.sum), drink from (select sum(tRev), drink from cNew group by drink order by drink) d group by drink").show()
      //spark.sql("select drink, rev from cRev where rev in (select max(rev) from cRev)").show()

      spark.sql("drop table if exists allRev")
      spark.sql("create table if not exists allRev (drink String, branch String, count int, rev decimal (10, 2))")
      spark.sql("insert into table allRev select * from aNew UNION ALL select * from bNew \n" +
        "UNION ALL select * from cNew")
      println("This is the table of all the drinks with the money that they made.")
      spark.sql("select * from allRev").show()
      println("The drink that made the most money is:")
      spark.sql("select drink, branch, rev from allRev where rev in (select max(rev) from allRev)").show()
      println("The drink that made the least amount of money is: ")
      spark.sql("select DISTINCT drink, branch, rev from allRev where rev in (select min(rev) from allRev)").show()


      startOver()
    }

    //METHOD THAT DOES WHAT MAIN DOES
    def mainAgain(): Unit = {
      val optionMap = ListMap((1 -> "p1"), (2 -> "p2"), (3 -> "p3"), (4 -> "p4"), (5 -> "p5"), (6 -> "p6"), (7 -> "QUIT"))
      val menu = new Menu(optionMap)
      menu.printMenu()
      val test = readLine("Enter a number for your option:").toInt
      val options = menu.selectOption(test)
      options match {
        case "p1" => p1func()
        case "p2" => p2func()
        case "p3" => p3func()
        case "p4" => p4func()
        case "p5" => p5func()
        case "p6" => p6func()
        case "QUIT" => println("Goodbye.")
        case _ => caseFailBeginning() //The default, catch-all
      }
    }

    def caseFailBeginning(): Unit = {
      println("Invalid option. Try again.")
      mainAgain()
    }

    @tailrec
    def startOver(): Unit = {
      println(
        """
          |Do you want to see the answer for another problem?
          |1 for YES, 2 for NO.
          |""".stripMargin)
      val answer = readLine("Enter a number for your answer: ")
      /*
      answer match {
        case "1" => mainAgain()
        case "2" => println("Goodbye.")
       */
      if (answer == "1"){
        mainAgain()
      }
      else if(answer == "2"){
        println("Goodbye.")
      }
      else{
        println("\nEnter a valid number.")
        startOver()
      }
    }

    println(
      """
        |This app solves the problems for Project 1.
        |A Coffee Shop has multiple branches where different drinks are served.
        |Enter the number for different problems to see information about
        |the data collected on the Coffee Shop's sales.
        |""".stripMargin)
    val optionMap = ListMap((1 -> "p1"), (2 -> "p2"), (3 -> "p3"), (4 -> "p4"), (5 -> "p5"), (6 -> "p6"), (7 -> "QUIT"))
    val menu = new Menu(optionMap)
    menu.printMenu()
    val test = readLine("Enter a number for your option:").toInt
    val options = menu.selectOption(test)
    options match{
      case "p1" => p1func()
      case "p2" => p2func()
      case "p3" => p3func()
      case "p4" => p4func()
      case "p5" => p5func()
      case "p6" => p6func()
      case "QUIT" => println("Goodbye.")
      case _ => println("Oh no.") //The default, catch-all
    }

  }

}
