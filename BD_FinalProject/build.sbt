name := "BD_FinalProject"

version := "1.0p"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.1",
  "org.apache.hadoop" % "hadoop-common" % "3.2.1",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.2.1",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.spark" %% "spark-core" % "2.3.4",
)

// https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8"
