 version := "0.1.0-SNAPSHOT"

 scalaVersion := "2.13.8"

name := "amrTraining_sparkTask"
val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-sql" % "3.2.1"
)
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.12"
