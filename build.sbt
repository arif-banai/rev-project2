name := "rev-project2"

version := "0.1"

scalaVersion := "2.12.13"

lazy val scriptClasspath = Seq("*")

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.1"
libraryDependencies +="org.apache.spark" %% "spark-mllib" % "3.1.1"