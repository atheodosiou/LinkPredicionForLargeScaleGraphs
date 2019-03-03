package Supervised

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.io.StdIn.{readInt, readLine}


object App {
  def main(args: Array[String]): Unit = {
    println("++++++++++++++++++++++++++++++\n| Supervised Link Prediction |\n++++++++++++++++++++++++++++++\n")
    //Show only error messages
    Logger.getLogger("org").setLevel(Level.ERROR)

    if(args.length < 3){
      println("Wrong parameters!\nApp will be terminated...")
      System.exit(-1)
    }else{
      //The magic
      //Read parameters
      val executionCores = args(0).toInt
      val numPartitions = args(1).toInt
      val model = args(2).toString
      val nodes_csv_file_path = "resources/graph/nodes.csv"

      println("App is running "+model+" with "+executionCores+" cores and "+numPartitions+" partitions.\n")

      //Create SparkSession and SparkContext
      val spark = SparkSession.builder
        .appName("LinkPrediction")
        .master("local["+executionCores+"]").getOrCreate()
      val sc = spark.sparkContext

      //Set StructType schema for input data
      val schemaStruct = StructType(
        StructField("id", IntegerType) ::
          StructField("pubYear", IntegerType) ::
          StructField("title", StringType) ::
          StructField("authors", StringType) ::
          StructField("jurnal", StringType) ::
          StructField("abstract", StringType) :: Nil
      )

      println("Creating nodeDF from "+nodes_csv_file_path+"file using StructType.\n")
      val nodeDf = spark.read.option("header", false).schema(schemaStruct)
        .csv(nodes_csv_file_path)
        .repartition(numPartitions)
        .cache()
//        .na.drop()
      val totalNodes = nodeDf.count()

      println("Repartitioning nodesDF into "+nodeDf.rdd.getNumPartitions+" partitions\n")
      println("NodesDf's total entries:"+totalNodes)
    }

  }
}
