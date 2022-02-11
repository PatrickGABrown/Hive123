package Learning

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

    //-------------------------------------------------------------------------------------------------------------

    //BEV_BRANCH TABLES
    spark.sql("DROP TABLE IF EXISTS Bev_BranchA")
    spark.sql("DROP TABLE IF EXISTS Bev_BranchB")
    spark.sql("DROP TABLE IF EXISTS Bev_BranchC")
    spark.sql("create table if not exists Bev_BranchA(drink String, branch String) row format delimited fields terminated by ','");
    spark.sql("create table if not exists Bev_BranchB(drink String, branch String) row format delimited fields terminated by ','");
    spark.sql("create table if not exists Bev_BranchC(drink String, branch String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE Bev_BranchA")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE Bev_BranchB")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE Bev_BranchC")
    //spark.sql("SELECT count(*) FROM Bev_BranchA where branch = 'Branch1'").show()


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

    //spark.sql("SELECT count(drink) FROM ConsCountC where drink = 'Cold_MOCHA'").show()
    //spark.sql("SELECT sum(count) FROM ConsCountC where drink = 'Cold_MOCHA'").show()
    //spark.sql("SELECT count(drink) FROM ConsCountC").show()
    //spark.sql("SELECT sum(count) FROM ConsCountC").show()

    //-------------------------------------------------------------------------------------------------------------


    //ONE WORKS!!!!!!!!!!!!!!!
    /*
    //Trying Number 1 again
    spark.sql("DROP TABLE IF EXISTS Branch1drink")
    spark.sql("CREATE TABLE IF NOT EXISTS Branch1drink AS SELECT drink, tc FROM \n" +
    "(select ConsCountA.drink, sum(ConsCountA.count)tc from \n" +
    "(select drink, branch from \n" +
    "(select * from Bev_BranchA where branch = 'Branch1' UNION ALL select * from Bev_BranchB where branch = 'Branch1' \n" +
    "UNION ALL select * from Bev_BranchC where branch = 'Branch1')) A JOIN ConsCountA ON \n" +
    "A.drink = ConsCountA.drink GROUP BY ConsCountA.drink \n" +
      "UNION ALL \n" +
    "select ConsCountB.drink, sum(ConsCountB.count)tc from \n" +
    "(select drink, branch from \n" +
    "(select * from Bev_BranchA where branch = 'Branch1' UNION ALL select * from Bev_BranchB where branch = 'Branch1' \n" +
    "UNION ALL select * from Bev_BranchC where branch = 'Branch1')) B JOIN ConsCountB ON \n" +
    "B.drink = ConsCountB.drink GROUP BY ConsCountB.drink \n" +
      ")")
    //spark.sql("select * from Branch1drink order by drink").show()

    spark.sql("DROP TABLE IF EXISTS practiceA")
    spark.sql("CREATE TABLE IF NOT EXISTS practiceA AS select ConsCountA.drink, sum(ConsCountA.count) tc from \n" +
      "(select drink, branch from (select * from Bev_BranchA where branch = 'Branch1' UNION ALL select * from Bev_BranchB \n" +
      "where branch = 'Branch1' UNION ALL select * from Bev_BranchC where branch = 'Branch1')) A JOIN ConsCountA ON \n" +
    "A.drink = ConsCountA.drink GROUP BY ConsCountA.drink ORDER BY ConsCountA.drink")
    //spark.sql("select sum(tc) from practiceA").show()
    spark.sql("DROP TABLE IF EXISTS practiceB")
    spark.sql("CREATE TABLE IF NOT EXISTS practiceB AS select ConsCountB.drink, sum(ConsCountB.count) tc from \n" +
      "(select drink, branch from (select * from Bev_BranchA where branch = 'Branch1' UNION ALL select * from Bev_BranchB \n" +
      "where branch = 'Branch1' UNION ALL select * from Bev_BranchC where branch = 'Branch1')) B JOIN ConsCountB ON \n" +
      "B.drink = ConsCountB.drink GROUP BY ConsCountB.drink ORDER BY ConsCountB.drink")
    //spark.sql("select sum(tc) from practiceB").show()
    spark.sql("DROP TABLE IF EXISTS practiceC")
    spark.sql("CREATE TABLE IF NOT EXISTS practiceC AS select ConsCountC.drink, sum(ConsCountC.count) tc from \n" +
      "(select drink, branch from (select * from Bev_BranchA where branch = 'Branch1' UNION ALL select * from Bev_BranchB \n" +
      "where branch = 'Branch1' UNION ALL select * from Bev_BranchC where branch = 'Branch1')) C JOIN ConsCountC ON \n" +
      "C.drink = ConsCountC.drink GROUP BY ConsCountC.drink ORDER BY ConsCountC.drink")
    //spark.sql("select sum(tc) from practiceC").show()

    //CHECK BRANCH 2 FOR NUMBER 1
    spark.sql("DROP TABLE IF EXISTS 2practiceA")
    spark.sql("CREATE TABLE IF NOT EXISTS 2practiceA AS select ConsCountA.drink, sum(ConsCountA.count) tc from \n" +
      "(select drink, branch from (select * from Bev_BranchA where branch = 'Branch2' UNION ALL select * from Bev_BranchB \n" +
      "where branch = 'Branch2' UNION ALL select * from Bev_BranchC where branch = 'Branch2')) A JOIN ConsCountA ON \n" +
      "A.drink = ConsCountA.drink GROUP BY ConsCountA.drink ORDER BY ConsCountA.drink")
    //spark.sql("select sum(tc) from 2practiceA").show()
    spark.sql("DROP TABLE IF EXISTS 2practiceB")
    spark.sql("CREATE TABLE IF NOT EXISTS 2practiceB AS select ConsCountB.drink, sum(ConsCountB.count) tc from \n" +
      "(select drink, branch from (select * from Bev_BranchA where branch = 'Branch2' UNION ALL select * from Bev_BranchB \n" +
      "where branch = 'Branch2' UNION ALL select * from Bev_BranchC where branch = 'Branch2')) B JOIN ConsCountB ON \n" +
      "B.drink = ConsCountB.drink GROUP BY ConsCountB.drink ORDER BY ConsCountB.drink")
    //spark.sql("select sum(tc) from 2practiceB").show()
    spark.sql("DROP TABLE IF EXISTS 2practiceC")
    spark.sql("CREATE TABLE IF NOT EXISTS 2practiceC AS select ConsCountC.drink, sum(ConsCountC.count) tc from \n" +
      "(select drink, branch from (select * from Bev_BranchA where branch = 'Branch2' UNION ALL select * from Bev_BranchB \n" +
      "where branch = 'Branch2' UNION ALL select * from Bev_BranchC where branch = 'Branch2')) C JOIN ConsCountC ON \n" +
      "C.drink = ConsCountC.drink GROUP BY ConsCountC.drink ORDER BY ConsCountC.drink")
    //spark.sql("select sum(tc) from 2practiceC").show()

    //OLD BRANCH2 Q1 CODE:
    //Select all the beverages in Branch 2
    spark.sql("DROP TABLE IF EXISTS Branch2drink")
    spark.sql("CREATE TABLE IF NOT EXISTS Branch2drink AS SELECT * FROM Bev_BranchA WHERE branch = 'Branch2'")
    spark.sql("INSERT INTO TABLE Branch2drink SELECT * FROM Bev_BranchB WHERE branch = 'Branch2'")
    spark.sql("INSERT INTO TABLE Branch2drink SELECT * FROM Bev_BranchC WHERE branch = 'Branch3'")
    spark.sql("SELECT * FROM Branch2drink where ")

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

    //OLD ONE CODE
    spark.sql("DROP TABLE IF EXISTS Branch1drink")
    spark.sql("CREATE TABLE IF NOT EXISTS Branch1drink AS SELECT * FROM Bev_BranchA WHERE branch = 'Branch1'")
    spark.sql("INSERT INTO TABLE Branch1drink SELECT * FROM Bev_BranchB WHERE branch = 'Branch1'")
    spark.sql("INSERT INTO TABLE Branch1drink SELECT * FROM Bev_BranchC WHERE branch = 'Branch1'")
    //spark.sql("SELECT count(*) FROM Branch1drink").show()

    spark.sql("DROP TABLE IF EXISTS Branch1count")
    spark.sql("CREATE TABLE IF NOT EXISTS Branch1count (drink string, count int)")
    spark.sql("INSERT INTO TABLE Branch1count SELECT DISTINCT ConsCountA.drink, SUM(ConsCountA.count) FROM ConsCountA\n" +
      " INNER JOIN Branch1drink ON (Branch1drink.drink = ConsCountA.drink) WHERE Branch1drink.branch = 'Branch1' GROUP BY ConsCountA.drink")
    //spark.sql("SELECT * FROM Branch1count order by drink").show()

    spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountB.drink, SUM(ConsCountB.count) FROM Branch1drink\n" +
      " INNER JOIN ConsCountB ON (Branch1drink.drink = ConsCountB.drink) WHERE Branch1drink.branch = 'Branch1' GROUP BY ConsCountB.drink")
    spark.sql("INSERT INTO TABLE Branch1count SELECT ConsCountC.drink, SUM(ConsCountC.count) FROM Branch1drink\n" +
      " INNER JOIN ConsCountC ON (Branch1drink.drink = ConsCountC.drink) WHERE Branch1drink.branch = 'Branch1' GROUP BY ConsCountC.drink")

    //Get the sum of all the counts for Branch 1
    //println("The total number of consumers for branch 1 is ")
    //spark.sql("SELECT SUM(count) FROM Branch1count").show()

     */
    //-------------------------------------------------------------------------------------------------------------


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


    /*
    //OLD TABLE OF ALL A DRINKS AND COUNTS
    spark.sql("DROP TABLE IF EXISTS AdrinkandCount")
    spark.sql("CREATE TABLE IF NOT EXISTS AdrinkandCount (drink string, branch String, count int)")
    spark.sql("INSERT INTO TABLE AdrinkandCount SELECT ConsCountA.drink, Bev_BranchA.branch, ConsCountA.count FROM Bev_BranchA\n" +
      " INNER JOIN ConsCountA ON (Bev_BranchA.drink = ConsCountA.drink) GROUP BY ConsCountA.drink, Bev_BranchA.branch, ConsCountA.count ORDER BY ConsCountA.drink, Bev_BranchA.branch")
    //spark.sql("select sum(count) from AdrinkandCount where branch = 'Branch1'").show()
    //spark.sql("ALTER TABLE AdrinkandCount ADD COLUMN price DECIMAL(10, 2) AFTER count")
    //spark.sql("select * from AdrinkandCount").show()
    //UPDATE Statements not supported in Spark 3.0 and up!
    //spark.sql("UPDATE AdrinkandCount SET price = 4.00*(count) WHERE drink LIKE '%Special%'")
    //spark.sql("select sum(count) from CdrinkandCount where branch = 'Branch1'").show()

    //OLD TABLE FOR ALL DRINKS AND COUNTS FOR B
    spark.sql("DROP TABLE IF EXISTS BdrinkandCount")
    spark.sql("CREATE TABLE IF NOT EXISTS BdrinkandCount (drink string, branch String, count int)")
    spark.sql("INSERT INTO TABLE BdrinkandCount SELECT ConsCountB.drink, Bev_BranchB.branch, ConsCountB.count FROM Bev_BranchB\n" +
      " INNER JOIN ConsCountB ON (Bev_BranchB.drink = ConsCountB.drink) GROUP BY ConsCountB.drink, Bev_BranchB.branch, ConsCountB.count ORDER BY ConsCountB.drink, Bev_BranchB.branch")
    //spark.sql("ALTER TABLE BdrinkandCount ADD COLUMN price DECIMAL(10, 2) AFTER count")
    //spark.sql("select count(drink) from BdrinkandCount where drink = 'Cold_Lite'").show()

    //OLD TABLE OF ALL DRINKS AND COUNTS FOR C
    spark.sql("DROP TABLE IF EXISTS CdrinkandCount")
    spark.sql("CREATE TABLE IF NOT EXISTS CdrinkandCount (drink string, branch String, count int)")
    spark.sql("INSERT INTO TABLE CdrinkandCount SELECT ConsCountC.drink, Bev_BranchC.branch, ConsCountC.count FROM Bev_BranchC\n" +
      " INNER JOIN ConsCountC ON (Bev_BranchC.drink = ConsCountC.drink) GROUP BY ConsCountC.drink, Bev_BranchC.branch, ConsCountC.count ORDER BY ConsCountC.drink, Bev_BranchC.branch")
    spark.sql("ALTER TABLE CdrinkandCount ADD COLUMN price DECIMAL(10, 2) AFTER count")
    //spark.sql("select * from CdrinkandCount").show()

     */

    //NEW TABLE OF ALL A DRINKS AND COUNTS
    spark.sql("DROP TABLE IF EXISTS AdrinkandCount")
    spark.sql("CREATE TABLE IF NOT EXISTS AdrinkandCount (drink string, branch String, count int)")
    spark.sql("INSERT INTO TABLE AdrinkandCount SELECT drink, branch, tc from \n" +
      "(select ConsCountA.drink, branch, sum(ConsCountA.count)tc from \n" +
      "(select drink, branch from (select * from Bev_BranchA UNION ALL select * from Bev_BranchB UNION ALL select * from Bev_BranchC)) X \n" +
      " JOIN ConsCountA ON (X.drink = ConsCountA.drink) GROUP BY ConsCountA.drink, X.branch)")
    //spark.sql("SELECT sum(count) FROM AdrinkandCount where branch = 'Branch2'").show()
    spark.sql("ALTER TABLE AdrinkandCount ADD COLUMN price DECIMAL(10, 2) AFTER count")

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
    //spark.sql("select * from allRev order by drink").show(1200)
    //spark.sql("select drink, rev from allRev where rev in (select max(rev) from allRev)").show()

    //val col = Seq("drink", "branch", "count", "rev")
    //import spark.implicits._
    //val df = ""
    //df.write.csv.save("/input/allRev")

    //spark.sql("insert overwrite local directory 'input/final' \n" +
    //"ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select * from allRev")


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
    //NOT WORKING!

    //spark.sql("SET spark.hadoop.hive.exec.dynamic.partition.node=nonstrict")
    /*
    spark.sql("CREATE TABLE IF NOT EXISTS partitioned3Table AS SELECT drink, branch FROM \n" +
      "(SELECT * FROM Bev_BranchA WHERE branch = 'Branch1'\n" +
      " OR branch = 'Branch8' OR branch = 'Branch10' UNION \n" +
      "SELECT * FROM Bev_BranchB WHERE branch = 'Branch1'\n" +
      " OR branch = 'Branch8' OR branch = 'Branch10' UNION \n" +
      "SELECT * FROM Bev_BranchC WHERE branch = 'Branch1'\n" +
      " OR branch = 'Branch8' OR branch = 'Branch10') PARTITIONED BY (branch)" )

     */
    //spark.sql("SELECT * FROM Bev_BranchA")
    //spark.sql("DROP TABLE IF EXISTS partBranch")
    //spark.sql("CREATE TABLE IF NOT EXISTS partBranch(drink String) PARTITIONED BY (branch String)")
    //spark.sql("INSERT OVERWRITE TABLE partBranch PARTITION (branch) SELECT drink, branch FROM Bev_BranchA")


    //spark.sql("SELECT * FROM partitioned3Table").show()


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
    //spark.sql("SELECT * FROM Branch4and7View").show()

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
