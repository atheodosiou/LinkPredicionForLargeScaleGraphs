package Supervised

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, when}
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
          StructField("journal", StringType) ::
          StructField("abstract", StringType) :: Nil
      )

      import spark.implicits._

      //=========================================> Reading Data Sets <=========================================

      println("Creating nodeDF from "+nodes_csv_file_path+" file using StructType.\n")
      var nodeDf = spark.read.option("header", false).schema(schemaStruct)
        .csv(nodes_csv_file_path)
        .repartition(numPartitions)
        .cache()
//        .na.drop()

      //Fill nul values
      println("Fill null values with empty sting...\n")
      val nullAuthor = ""
      val nullJournal = ""
      val nullAbstract = ""

      nodeDf = nodeDf.na.fill(nullAuthor, Seq("authors"))
      nodeDf = nodeDf.na.fill(nullJournal, Seq("journal"))
      nodeDf = nodeDf.na.fill(nullAbstract, Seq("abstract"))

      nodeDf = nodeDf.withColumn("nonNullAbstract", when(
        col("abstract") === nullAbstract,
        col("title")
      ).otherwise(col("abstract")))
        .drop("abstract").withColumnRenamed("nonNullAbstract", "abstract")

      nodeDf = nodeDf.withColumn("nonNullJournal", when(
        col("journal") === nullJournal,
        col("title")
      ).otherwise(col("abstract")))
        .drop("journal").withColumnRenamed("nonNullJournal", "journal")

      nodeDf = nodeDf.withColumn("nonNullAuthors", when(
        col("authors") === nullAuthor,
        col("title")
      ).otherwise(col("authors")))
        .drop("authors").withColumnRenamed("nonNullAuthors", "authors")

      println("Showing sample of nodeDF after filling null values...\n")
      nodeDf.show(5)

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
      //========================================> Data Pre Processing <========================================

      println("Join nodeDf and trainingDF for source nodes as srcDf..\n")
      var srcDf = nodeDf.as("a").join(trainingDF.as("b"),$"a.id"===$"b.srcId").cache()

      println("Renaming srcDf...\n")
      srcDf= srcDf.drop("id")
        .withColumnRenamed("pubYear","year_from")
        .withColumnRenamed("title","title_from")
        .withColumnRenamed("authors","authors_from")
        .withColumnRenamed("journal","journal_from")
        .withColumnRenamed("abstract","abstract_from")
        .withColumnRenamed("srcId","id_from")

      println("Join nodeDf and srcDf for destination nodes as dstDf...\n")
      var dstDf = nodeDf.as("a").join(srcDf.as("b"),$"a.id"===$"b.dstId").cache()

      println("Renaming dstDf...\n")
      dstDf= dstDf.drop("id")
        .withColumnRenamed("pubYear","year_to")
        .withColumnRenamed("title","title_to")
        .withColumnRenamed("authors","authors_to")
        .withColumnRenamed("journal","journal_to")
        .withColumnRenamed("abstract","abstract_to")
        .withColumnRenamed("dstId","id_to")

      println("Selecting columns from dstDf as new dataframe with name joinedDf...\n")
      var joinedDf= dstDf.select(
        "id_from","id_to","label","year_from","year_to",
        "title_from","title_to","authors_from","authors_to","journal_from",
        "journal_to","abstract_from","abstract_to"
      )

      println("Showing sample of joinedDF...\n")
      joinedDf.show(5)

      joinedDf.createOrReplaceTempView("data")

      println("Calculating dist time (year from - year to)...")
      joinedDf = spark.sqlContext.sql(
        "select id_from, id_to, label, year_from, year_to, " +
          "title_from, title_to, authors_from, authors_to, journal_from, " +
          "journal_to, abstract_from, abstract_to, abs(year_from - year_to) from data"
      )
      joinedDf = joinedDf.withColumnRenamed(
        "abs((year_from - year_to))","time_dist"
      )

      joinedDf.show(5)

      /*
      Page rank code goes here but experiments shows that it was not affected the final result and
      it is time consumming to calculte it for each node in the network, so we will skip it!
      */

      //Tokenization using sql functions

      println("Spliting title_from column into words...")
      joinedDf = joinedDf.withColumn("title_from_words", functions.split(col("title_from"), "\\s+"))
      println("Spliting title_to column into words...")
      joinedDf = joinedDf.withColumn("title_to_words", functions.split(col("title_to"), "\\s+"))

      println("Spliting authors_from column into words...")
      joinedDf = joinedDf.withColumn("authors_from_words", functions.split(col("authors_from"), ","))
      println("Spliting authors_to column into words...")
      joinedDf = joinedDf.withColumn("authors_to_words", functions.split(col("authors_to"), ","))

      println("Spliting journal_from column into words...")
      joinedDf = joinedDf.withColumn("journal_from_words", functions.split(col("journal_from"), "\\s+"))
      println("Spliting journal_to column into words...")
      joinedDf = joinedDf.withColumn("journal_to_words", functions.split(col("journal_to"), "\\s+"))

      println("Spliting abstract_from column into words...")
      joinedDf = joinedDf.withColumn("abstract_from_words", functions.split(col("abstract_from"), "\\s+"))
      println("Spliting abstract_to column into words...")
      joinedDf = joinedDf.withColumn("abstract_to_words", functions.split(col("abstract_to"), "\\s+"))

      println("Showing sample of joinedDF after splitting columns into words...\n")
      joinedDf.show(5)

      //Removing stop words

      println("Removing stop words from title_from column...")
      val remover = new StopWordsRemover().setInputCol("title_from_words").setOutputCol("title_from_words_f")
      joinedDf = remover.transform(joinedDf)

      println("Removing stop words from title_to column...")
      val remover2 = new StopWordsRemover().setInputCol("title_to_words").setOutputCol("title_to_words_f")
      joinedDf = remover2.transform(joinedDf)

      println("Removing stop words from authors_from column...")
      val remover3 = new StopWordsRemover().setInputCol("authors_from_words").setOutputCol("authors_from_words_f")
      joinedDf = remover3.transform(joinedDf)

      println("Removing stop words from authors_to column...")
      val remover4 = new StopWordsRemover().setInputCol("authors_to_words").setOutputCol("authors_to_words_f")
      joinedDf = remover4.transform(joinedDf)

      println("Removing stop words from journal_from column...")
      val remover8 = new StopWordsRemover().setInputCol("journal_from_words").setOutputCol("journal_from_words_f")
      joinedDf = remover8.transform(joinedDf)

      println("Removing stop words from journal_to column...")
      val remover9 = new StopWordsRemover().setInputCol("journal_to_words").setOutputCol("journal_to_words_f")
      joinedDf = remover9.transform(joinedDf)

      println("Removing stop words from abstract_from column...")
      val remover5 = new StopWordsRemover().setInputCol("abstract_from_words").setOutputCol("abstract_from_words_f")
      joinedDf = remover5.transform(joinedDf)

      println("Removing stop words from abstract_to column...")
      val remover6 = new StopWordsRemover().setInputCol("abstract_to_words").setOutputCol("abstract_to_words_f")
      joinedDf = remover6.transform(joinedDf)

      println("Showing sample and schema of joinedDF after removing stop words from each column...")
      joinedDf.show(5)
      joinedDf.printSchema()

    }
  }
}
