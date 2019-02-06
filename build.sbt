name := "modeler"

version := "1.0.1"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.8.0" % "provided"

libraryDependencies += "spark.jobserver" %% "job-server-extras" % "0.8.0" % "provided"