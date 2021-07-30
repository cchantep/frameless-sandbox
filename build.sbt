ThisBuild / organization := "cchantep"

// Format and style
ThisBuild / scalafmtOnCompile := true

inThisBuild(
  List(
    //scalaVersion := "2.13.3",
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    scalafixDependencies ++= Seq(
      "com.github.liancheng" %% "organize-imports" % "0.5.0")
  )
)

ThisBuild / resolvers ++= Seq(
  Resolver.sonatypeRepo("staging"),
  "confluent" at "https://packages.confluent.io/maven/"
)

ThisBuild / libraryDependencies ++= {
  val specsVer = "4.12.3"

  Seq("core", "junit").map { n =>
    "org.specs2" %% s"specs2-${n}" % specsVer % Test
  }
}

// Ingestion
val sparkVer = "3.0.1"
val hadoopVer = "3.2.0" // TODO: hadoop-native

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVer % Provided,
  "org.apache.spark" %% "spark-sql-kafka-0-10"  % sparkVer,
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1")

val ingestionDependencies = sparkDependencies ++ Seq(
  // Hadoop
  ("org.apache.hadoop" % "hadoop-aws" % hadoopVer).
    exclude("com.amazonaws", "aws-java-sdk-bundle"),
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.1034",
  "org.apache.hadoop" % "hadoop-common" % hadoopVer % Provided,

  // Other
  "com.typesafe" % "config" % "1.4.1")

lazy val root = (project in file(".")).settings(
  name := "frameless-sandbox",
  description := "Frameless sandbox",
  scalaVersion := Compiler.scala212,

  libraryDependencies ++= ingestionDependencies ++ Seq(
    "org.typelevel" %% "frameless-dataset" % "0.10.1",

    "com.sksamuel.avro4s" %% "avro4s-core" % "4.0.4" % Test
  ),
  libraryDependencies ~= {
    _.map(_.exclude("org.apache.avro", "*"))
  }
)
