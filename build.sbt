name := "Spark_Tutorial"

version := "0.1"

scalaVersion := "2.11.12"

assemblyMergeStrategy in assembly :=
  {
    case PathList("reference.conf") => MergeStrategy.concat
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.3"


// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.3"

