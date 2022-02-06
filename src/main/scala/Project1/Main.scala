package Project1

import Project1.MenuDetails._
import scala.io.StdIn.readLine


object Main {

  def main (args: Array[String]): Unit = {
    def p1func(): Unit = {
      println("Hello.")
    }

    val optionMap = Map((1 -> "p1"), (2 -> "p2"), (3 -> "p3"))
    val menu = new Menu(optionMap)
    menu.printMenu()
    val test = readLine("Enter a number for your option:").toInt
    val options = menu.selectOption(test)
    options match{
      case "p1" => p1func()
    }

  }

}
