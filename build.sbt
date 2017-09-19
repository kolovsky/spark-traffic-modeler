name := "modeler"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.3"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.7.0" % "provided"

libraryDependencies += "spark.jobserver" %% "job-server-extras" % "0.7.0" % "provided"