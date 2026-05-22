import org.apache.logging.log4j.core.config.composite.MergeStrategy

name := "StreamFlixAnalytics"
version := "0.1"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0"
)
dependencyOverrides += "org.scala-lang" % "scala-library" % "2.12.15"
