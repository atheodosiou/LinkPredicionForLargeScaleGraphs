package LinkPrediction.Supervised

import LinkPrediction.Utilities
import com.rockymadden.stringmetric.similarity.JaccardMetric
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, GraphLoader}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ChiSqSelector, NGram, StopWordsRemover, VectorAssembler}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.io.StdIn.{readInt, readLine}


object App {
  def main(args: Array[String]): Unit = {
    println("++++++++++++++++++++++++++++++\n| LinkPrediction.Supervised Link Prediction |\n++++++++++++++++++++++++++++++\n")
    //Show only error messages
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("RUNNING WITH LIMIT ON DATASET!!!")

    if(args.length < 3){
      println("Wrong parameters!\nApp will be terminated...")
      System.exit(-1)
    }else{
      //The magic
      //Read parameters
      val executionCores = args(0).toInt
      val numPartitions = args(1).toInt
      val model = args(2).toString
      val trainingPercentage = args(3).toDouble

      //Values
      val nodes_csv_file_path = "resources/graph/nodes.csv"
      val training_set_file_path = "resources/supplementary_files/training_set.txt"
      val test_set_file_path = "resources/supplementary_files/test_set.txt"
      val testPercentage = 1 -trainingPercentage

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
        .repartition(numPartitions).limit(500)
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

      println("Removing stop words from each column...")
      val remover1 = new StopWordsRemover().setInputCol("title_from_words").setOutputCol("title_from_words_f")
      val remover2 = new StopWordsRemover().setInputCol("title_to_words").setOutputCol("title_to_words_f")
      val remover3 = new StopWordsRemover().setInputCol("authors_from_words").setOutputCol("authors_from_words_f")
      val remover4 = new StopWordsRemover().setInputCol("authors_to_words").setOutputCol("authors_to_words_f")
      val remover5 = new StopWordsRemover().setInputCol("journal_from_words").setOutputCol("journal_from_words_f")
      val remover6 = new StopWordsRemover().setInputCol("journal_to_words").setOutputCol("journal_to_words_f")
      val remover7 = new StopWordsRemover().setInputCol("abstract_from_words").setOutputCol("abstract_from_words_f")
      val remover8 = new StopWordsRemover().setInputCol("abstract_to_words").setOutputCol("abstract_to_words_f")

      //N-Grams
      //If the input is smaller than the N, no output is produced. E.g., Authors column,
      //for a paper with 1 author, the N-Gram with N=2, will be []

      val nGrams = 2
      println("Creating N-Grams for dataframe with N=: "+nGrams)
      val ngram1 = new NGram().setN(nGrams).setInputCol("title_from_words_f").setOutputCol("title_from_n_grams")
      val ngram2 = new NGram().setN(nGrams).setInputCol("title_to_words_f").setOutputCol("title_to_n_grams")
      val ngram3 = new NGram().setN(nGrams).setInputCol("authors_from_words_f").setOutputCol("authors_from_n_grams")
      val ngram4 = new NGram().setN(nGrams).setInputCol("authors_to_words_f").setOutputCol("authors_to_n_grams")
      val ngram5 = new NGram().setN(nGrams).setInputCol("journal_from_words_f").setOutputCol("journal_from_n_grams")
      val ngram6 = new NGram().setN(nGrams).setInputCol("journal_to_words_f").setOutputCol("journal_to_n_grams")
      val ngram7 = new NGram().setN(nGrams).setInputCol("abstract_from_words_f").setOutputCol("abstract_from_n_grams")
      val ngram8 = new NGram().setN(nGrams).setInputCol("abstract_to_words_f").setOutputCol("abstract_to_n_grams")

      println("Setting pipeline stages...")
      val stages = Array(
        remover1,remover2,remover3,remover4,
        remover5,remover6,remover7,remover8,
        ngram1,ngram2,ngram3,ngram4,ngram5,
        ngram6,ngram7,ngram8
      )
      val pipeline = new Pipeline()
      pipeline.setStages(stages)

      println("Transforming dataframe via pipelineModel.transform() method\n")
      val pModel = pipeline.fit(joinedDf)
      joinedDf = pModel.transform(joinedDf)

      val utils = new Utilities();
      val udf_words_overlap = udf(utils.findNumberOfCommonWords(_:Seq[String], _:Seq[String]))
      val udf_string_jaccard_similarity=udf(JaccardMetric(1).compare(_:String, _:String))

      // WORD OVERLAPING
      println("Getting the number of common words between title_from and title_to columns using UDF function...")
      joinedDf = joinedDf.withColumn("titles_intersection",udf_words_overlap(joinedDf("title_from_words_f"),joinedDf("title_to_words_f")))
      println("Getting the number of common words between authors_from and authors_to columns using UDF function...")
      joinedDf = joinedDf.withColumn("authors_intersection",udf_words_overlap(joinedDf("authors_from_words_f"),joinedDf("authors_to_words_f")))
      println("Getting the number of common words between journal_from and journal_to columns using UDF function...")
      joinedDf = joinedDf.withColumn("journal_intersection",udf_words_overlap(joinedDf("journal_from_words_f"),joinedDf("journal_to_words_f")))
      println("Getting the number of common words between abstract_from and abstract_to columns using UDF function...")
      joinedDf = joinedDf.withColumn("abstract_intersection",udf_words_overlap(joinedDf("abstract_from_words_f"),joinedDf("abstract_to_words_f")))

      //JACCARD SIMILARITIES
      println("Getting Jaccard similarity between title_from and title_to using UDF function...")
      joinedDf = joinedDf.withColumn("titles_jaccard",udf_string_jaccard_similarity(joinedDf("title_from"),joinedDf("title_to")))
      println("Getting Jaccard similarity between authors_from and authors_to using UDF function...")
      joinedDf = joinedDf.withColumn("authors_jaccard",udf_string_jaccard_similarity(joinedDf("authors_from"),joinedDf("authors_to")))
      println("Getting Jaccard similarity between journal_from and journal_to using UDF function...")
      joinedDf = joinedDf.withColumn("journal_jaccard",udf_string_jaccard_similarity(joinedDf("journal_from"),joinedDf("journal_to")))
      println("Getting Jaccard similarity between abstract_from and abstract_to using UDF function...")
      joinedDf = joinedDf.withColumn("abstract_jaccard",udf_string_jaccard_similarity(joinedDf("abstract_from"),joinedDf("abstract_to")))

      //Something from GraphX
      println("Creating graph from training_set.txt to get structural features...")
      val graph = GraphLoader.edgeListFile(sc,"resources/supplementary_files/training_set.txt")
      println("Collecting neighbors for each node of the graph based on the edge list...")
      val neighbours = graph.collectNeighbors(EdgeDirection.Either).map(x=>{
        (x._1, x._2.map(x=>{x._1}))
      }).toDF("node_id","neighbours")
      println("Showing a sample of collected neighbors...\n")
      neighbours.show(5,false)

      var neighboursDF = trainingDF.as("a").join(neighbours.as("b"),$"a.srcId" === $"b.node_id").withColumnRenamed("neighbours","neighbours_from")
      neighboursDF = neighboursDF.as("a").join(neighbours.as("b"),$"a.dstId" === $"b.node_id").drop("node_id").withColumnRenamed("neighbours","neighbours_to")

      //Common neighbors
      println("Calculating common neighbours for every src dst pair...")
      val udf_common_neighbours = udf(utils.findNumberOfCommonNeighbours(_:Seq[String], _:Seq[String]))
      neighboursDF = neighboursDF.withColumn(
        "common_neighbours",
        udf_common_neighbours(
          neighboursDF("neighbours_from"),
          neighboursDF("neighbours_to")
        )
      ).drop(col("label")).cache()
      neighboursDF.show(5)

      //Triangle counting - too costly, not so affective
//      println("Calculating triangles count for every src dst separetly...")
//      val triangles = graph.triangleCount().vertices
//      val trianglesDF = triangles.toDF("node_id","triangles_count")

//      var newDF = neighboursDF.as("a").join(trianglesDF.as("b"),$"a.srcId" === $"b.node_id").withColumnRenamed("triangles_count","triangles_count_from").cache()
//      newDF = newDF.as("a").join(trianglesDF.as("b"),$"a.dstId" === $"b.node_id").withColumnRenamed("triangles_count","triangles_count_to").drop("node_id").cache()
//      newDF.show(5)

      println("Adding stractural features to final df..")
      var featuresDF = joinedDf.as("a").join(neighboursDF.as("b"),$"a.id_from" ===$"b.srcId" && $"a.id_to" === $"b.dstId").cache()

      println("Spiting dataset into training and test...")
      val Array(trainingData, testData) = featuresDF.randomSplit(Array(0.7,0.3))
      println("Data splited for training: "+trainingPercentage+" and for testing: "+testPercentage)

      println("Assembling features...")
      val assembler = new VectorAssembler()
        .setInputCols(Array(
        "titles_intersection","authors_intersection","journal_intersection",
         "abstract_intersection","time_dist", "titles_jaccard","authors_jaccard",
         "journal_jaccard","abstract_jaccard","common_neighbours"
//         ,"triangles_count_from","triangles_count_to"
      )).setOutputCol("features")

      featuresDF = assembler.transform(featuresDF).select(col("features"),col("label")).cache()
      println("Showing sample of final features form...")
      featuresDF.show(5)

      //Chi-Squared test of independence to decide which features to choose.

      println("Running Chi-Squared test of independence to decide which features to choose...")
      val selector = new ChiSqSelector()
        .setNumTopFeatures(1)
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setOutputCol("selectedFeatures")

      val result = selector.fit(featuresDF)

      val selectedFeatures = result.selectedFeatures

      val finalResult = result.transform(featuresDF)

      println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
      finalResult.show(false)
      println("Selected Features:")
      selectedFeatures.foreach(println)

    }
  }
}
