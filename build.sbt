version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.8"

name := "amrTraining_sparkTask"
val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"
)
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
