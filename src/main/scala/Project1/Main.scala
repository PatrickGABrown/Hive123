package Project1

import Project1.MenuDetails._
import scala.io.StdIn.readLine
import org.apache.spark.sql.SparkSession


object Main {

  def main (args: Array[String]): Unit = {

    //first things first, start a spark session
    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    //CREATING TABLES FOR BRANCH DRINKS
    /*
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
     */


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
      spark.sql("INSERT INTO TABLE Branch2drink SELECT * FROM Bev_BranchC WHERE branch = 'Branch3'")
      //Create a table with the counts of ALL the drinks in Branch 2
      spark.sql("DROP TABLE IF EXISTS Branch2count")
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
      //Use a limit and a desc to limit it to only the top answer.
      println("The most consumed beverage on Branch 2 is ")
      spark.sql("SELECT drink, SUM(count) FROM Branch2count GROUP BY drink ORDER BY SUM(count) DESC LIMIT 1").show()

      println("The average consumed beverage from Branch 2 is ")
      spark.sql("SELECT drink, AVG(count) FROM Branch2count GROUP BY drink").show()
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
      spark.sql("CREATE TABLE IF NOT EXISTS AllBranchDrinks(drink String, branch String) row format delimited fields terminated by ','");
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE AllBranchDrinks")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE AllBranchDrinks")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE AllBranchDrinks")
      println("The common beverages available in Branches 4 and 7 are ")
      spark.sql("SELECT DISTINCT(drink) FROM AllBranchDrinks WHERE drink IN\n" +
        " (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch4')\n" +
        " AND drink IN (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch7')").show()
      //Call method to start the app over
      startOver()
    }

    //QUESTION 4 METHOD:
    def p4func(): Unit = {
      println("QUESTION 4 ANSWERS:")
      /*
      //Create partition on scenario 3
      //NOT WORKING!
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
      spark.sql("ALTER TABLE Bev_BranchA SET tblproperties('notes' = 'These are the drinks offered in each branch in the A group')")
      //spark.sql("ALTER TABLE Bev_BranchB SET tblproperties('notes' = 'These are the drinks offered in each branch in the B group')")
      //spark.sql("ALTER TABLE Bev_BranchC SET tblproperties('notes' = 'These are the drinks offered in each branch in the C group')")
      //spark.sql("ALTER TABLE ConsCountA SET tblproperties('notes' = 'This is the consumer count for group A')")
      //spark.sql("ALTER TABLE ConsCountB SET tblproperties('notes' = 'This is the consumer count for group B')")
      //spark.sql("ALTER TABLE ConsCountB SET tblproperties('notes' = 'This is the consumer count for group C')")

      spark.sql("ALTER TABLE Bev_BranchA SET tblproperties('comments' = 'This is a comment.')")
      //spark.sql("ALTER TABLE Bev_BranchB SET tblproperties('comments' = '')")
      //spark.sql("ALTER TABLE Bev_BranchC SET tblproperties('comments' = '')")
      //spark.sql("ALTER TABLE ConsCountA SET tblproperties('comments' = '')")
      //spark.sql("ALTER TABLE ConsCountB SET tblproperties('comments' = '')")
      //spark.sql("ALTER TABLE ConsCountC SET tblproperties('comments' = '')")

      println("New table properties added to Bev_BranchA table: ")
      spark.sql("show tblproperties Bev_BranchA").show()
      //spark.sql("show tblproperties Bev_BranchB").show()
      //spark.sql("show tblproperties Bev_BranchC").show()
      //spark.sql("show tblproperties ConsCountA").show()
      //spark.sql("show tblproperties ConsCountB").show()
      //spark.sql("show tblproperties ConsCountC").show()

      //Remove a row from a table

      spark.sql("DROP TABLE IF EXISTS AllBranchDrinks")
      spark.sql("CREATE TABLE IF NOT EXISTS AllBranchDrinks(drink String, branch String) row format delimited fields terminated by ','");
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE AllBranchDrinks")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE AllBranchDrinks")
      spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE AllBranchDrinks")
      spark.sql("DROP VIEW IF EXISTS Branch4and7DrinksView")
      spark.sql("CREATE VIEW IF NOT EXISTS Branch4and7DrinksView AS SELECT DISTINCT(drink) FROM AllBranchDrinks WHERE drink IN\n" +
        " (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch4')\n" +
        " AND drink IN (SELECT drink FROM AllBranchDrinks WHERE branch = 'Branch7')")
      spark.sql("select * from Branch4and7DrinksView").show()
      /*
      spark.sql("SELECT * FROM Branch4and7DrinksView LEFT JOIN \n" +
      "(SELECT * FROM Branch4and7DrinksView WHERE drink = 'Triple_cappuccino' ORDER BY drink DESC LIMIT 1) B ON \n +" +
        "Branch4and7DrinksView.drink = B.drink WHERE B.drink IS NULL").show()
       */


      //Call method to start app over:
      startOver()
    }

    def p6func(): Unit = {
      println("FUTURE QUERY!")
      startOver()
    }

    def mainAgain(): Unit = {
      val optionMap = Map((1 -> "p1"), (2 -> "p2"), (3 -> "p3"), (4 -> "p4"), (5 -> "p5"), (6 -> "p6"))
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
      }
    }

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
    val optionMap = Map((1 -> "p1"), (2 -> "p2"), (3 -> "p3"), (4 -> "p4"), (5 -> "p5"), (6 -> "p6"))
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
    }

  }

}
