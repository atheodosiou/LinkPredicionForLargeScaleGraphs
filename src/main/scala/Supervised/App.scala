package Supervised

import org.apache.log4j.{Level, Logger}


object App {
  def main(args: Array[String]): Unit = {
    println("++++++++++++++++++++++++++++++\n| Supervised Link Prediction |\n++++++++++++++++++++++++++++++\n")
    Logger.getLogger("org").setLevel(Level.ERROR)
  }
}
