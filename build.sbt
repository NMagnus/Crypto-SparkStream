name := "Crypto-SparkStream"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq( 
  "com.typesafe.akka" %% "akka-stream" % "2.5.21",
  "com.typesafe.akka" %% "akka-http" % "10.1.8",
  "com.typesafe.akka" %% "akka-actor" % "2.5.21",
)