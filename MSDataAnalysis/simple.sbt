name := "MS Data Analysis"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.10"              % "1.1.0" % "provided",
  "org.apache.spark"  % "spark-mllib_2.10"             % "1.1.0"
  )
