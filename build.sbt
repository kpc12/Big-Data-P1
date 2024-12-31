ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "projectDelta"
  )

assembly / assemblyMergeStrategy := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.3",
  "org.apache.spark" %% "spark-core" % "3.4.3",
  "io.delta" %% "delta-core" % "2.4.0",
  "org.apache.hadoop" % "hadoop-client" % "3.3.1",
  "org.apache.hadoop" % "hadoop-common" % "3.3.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.7.1" % "provided",
  "org.json4s" %% "json4s-jackson" % "3.6.10" % "provided",
  "com.typesafe" % "config" % "1.4.2",
  "org.scalaj" %% "scalaj-http" % "2.4.2"
)




resolvers += "Delta Lake Repository" at "https://packages.delta.io/maven"

