name := "spark-CGrad"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "3.1.1"

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-core" % sparkVersion
)
