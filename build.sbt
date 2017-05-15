import _root_.sbtassembly.Plugin.AssemblyKeys
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

assemblySettings

name := "UserProfile"

version := "1.0"

scalaVersion := "2.11.8"

jarName in assembly := "userprofile.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.2" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.0.2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.0.2" % "provided",
//  "org.json4s" % "json4s-native_2.11" % "3.5.0" % "provided",
  "com.github.scopt" % "scopt_2.11" % "3.3.0",
  "com.googlecode.combinatoricslib" % "combinatoricslib" % "2.1"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case "META-INF/maven/org.apache.avro/avro-ipc/pom.properties" => MergeStrategy.last
  case "META-INF/maven/org.slf4j/slf4j-api/pom.properties" => MergeStrategy.last
  case "META-INF/maven/org.slf4j/slf4j-api/pom.xml" => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case x => old(x)
}
}