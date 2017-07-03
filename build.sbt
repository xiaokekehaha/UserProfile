import _root_.sbtassembly.Plugin.AssemblyKeys
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._


// scalastyle:off
assemblySettings

name := "UserProfile"

version := "1.0"

scalaVersion := "2.11.8"

jarName in assembly := "userprofile.jar"

// libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
//  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided",
//  "org.apache.spark" %% "spark-hive" % "1.6.1" % "provided",
//  "org.apache.spark" %% "spark-mllib" % "1.6.1" % "provided",
//   "org.apache.hive" % "hive-exec" % "1.1.0-cdh5.5.2",
//  "com.github.scopt" % "scopt_2.11" % "3.3.0",
//  "mysql" % "mysql-connector-java" % "5.1.36",
//  "com.googlecode.combinatoricslib" % "combinatoricslib" % "2.1"
// )

//resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases"
//
//resolvers += "Thrift" at "http://people.apache.org/~rawson/repo/"

libraryDependencies ++= Seq(
  "io.searchbox" % "jest" % "1.0.3",
  "org.json4s" % "json4s-native_2.11" % "3.5.0",
  "org.apache.hadoop" % "hadoop-core" % "0.20.2",
  "org.apache.hbase" % "hbase" % "0.90.4",
//  "org.apache.hbase" % "hbase-common" % "1.2.5" % "provided",
//  "org.apache.hbase" % "hbase-client" % "1.2.5" % "provided",
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.0.2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.1.1" % "provided"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case "META-INF/maven/org.apache.avro/avro-ipc/pom.properties" => MergeStrategy.last
  case "META-INF/maven/org.slf4j/slf4j-api/pom.properties" => MergeStrategy.last
  case "META-INF/maven/org.slf4j/slf4j-api/pom.xml" => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("org", "objenesis", xs @ _*) => MergeStrategy.last
  case PathList("org","joda", xs @ _*) => MergeStrategy.last
  case "org/objenesis/Objenesis.class" => MergeStrategy.last
  case "META-INF/maven/com.google.guava/guava/pom.properties" => MergeStrategy.first
  case "META-INF/maven/com.google.guava/guava/pom.xml" => MergeStrategy.first
  case "overview.html" => MergeStrategy.first
  case "parquet.thrift" => MergeStrategy.first
  case "plugin.xml" => MergeStrategy.first
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case PathList("io","netty", xs @ _*) => MergeStrategy.first
  case PathList("javax","servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax","ws", xs @ _*) => MergeStrategy.last
  case PathList("javax","xml", xs @ _*) => MergeStrategy.last
  case x => old(x)
}
}
