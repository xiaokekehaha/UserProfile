import _root_.sbtassembly.Plugin.AssemblyKeys
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

assemblySettings

name := "UserProfile"

version := "1.0"

scalaVersion := "2.11.8"

jarName in assembly := "SparkLocal.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.2" ,
  "org.apache.spark" %% "spark-sql" % "2.0.2" ,
  "com.github.scopt" % "scopt_2.11" % "3.3.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.2",
  "org.json4s" % "json4s-native_2.11" % "3.5.0",
  "com.googlecode.combinatoricslib" % "combinatoricslib" % "2.1"
)