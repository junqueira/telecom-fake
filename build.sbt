name := "quality"

version := "1.0"

scalaVersion := "2.11.8"

// === Spark === //
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "2.1.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.1" % "provided"

// === Play Json === //
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.9"

// === Storage - AWS === //
libraryDependencies += "jp.co.bizreach" %% "aws-s3-scala" % "0.0.13"
