package LinkPrediction.Unsupervised

import org.apache.log4j.{Level, Logger}
import scala.io.StdIn.{readLine}

object App {
  def main(args: Array[String]): Unit = {
    println("++++++++++++++++++++++++++++++++\n| LinkPrediction.Unsupervised Link Prediction |\n++++++++++++++++++++++++++++++++\n")
    //Show only error messages
    Logger.getLogger("org").setLevel(Level.ERROR)

    val s = readLine("Running local? (yes/no)\n")
    println("You said: '" + s + "'")

  }
}
