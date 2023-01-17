ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "firstCode"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.2"
libraryDependencies +=  "org.apache.spark" %% "spark-sql" % "3.2.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.2"

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1207.jre7"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.1.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.1.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.1.2"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.12.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.12.7"


// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1207.jre7"
