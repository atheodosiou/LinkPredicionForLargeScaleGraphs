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

      //Values
      val nodes_csv_file_path = "resources/graph/nodes.csv"
      val training_set_file_path = "resources/supplementary_files/training_set.txt"
      val test_set_file_path = "resources/supplementary_files/test_set.txt"

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

      import spark.implicits._

      //=========================================> Reading Data Sets <=========================================

      println("Creating nodeDF from "+nodes_csv_file_path+" file using StructType.\n")
      val nodeDf = spark.read.option("header", false).schema(schemaStruct)
        .csv(nodes_csv_file_path)
        .repartition(numPartitions)
        .cache()
//        .na.drop()
      val totalNodes = nodeDf.count()

      println("Repartitioning nodesDF into "+nodeDf.rdd.getNumPartitions+" partitions\n")
      println("NodesDf's total entries:"+totalNodes+"\n")

      println("Showing sample of nodesDF...\n")
      nodeDf.show(5,false)

      println("Reading training data from "+ training_set_file_path+" file...\n")
      val trainingDF = sc.textFile(training_set_file_path).map(line =>{
        val fields = line.split(" ")
        (fields(0),fields(1), fields(2).toInt)
      }).toDF("srcId","dstId","label").repartition(numPartitions)
        .cache()

      val totalTrainingEdges = trainingDF.count()

      println("Repartitioning trainingDF into "+trainingDF.rdd.getNumPartitions+" partitions\n")
      println("TrainingDF's total entries:"+totalTrainingEdges+"\n")

      println("Showing sample of trainingDF...\n")
      trainingDF.show(5,false)

      println("Reading test data from "+test_set_file_path +" file...\n")
      val testDF = sc.textFile(test_set_file_path).map(line =>{
        val fields = line.split(" ")
        (fields(0),fields(1))
      }).toDF("srcId","dstId").repartition(numPartitions)
        .cache()

      val totalTestEdges = testDF.count()

      println("Repartitioning testDF into "+testDF.rdd.getNumPartitions+" partitions\n")
      println("TestDF's total entries:"+totalTestEdges +"\n")

      println("Showing sample of testDF...\n")
      testDF.show(5,false)

      //=======================================================================================================

      //=========================================> Reading Data Sets <=========================================
    }
  }
}
