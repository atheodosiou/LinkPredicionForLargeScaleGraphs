name := "LinkPredictionForLargScaleGraphs"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.0" //2.3

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
//  "com.github.vickumar1981" %% "stringdistance" % "1.0.7",
  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4"
)