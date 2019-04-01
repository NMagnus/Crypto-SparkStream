name := "Crypto-SparkStream"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq( 
  "com.typesafe.akka" %% "akka-stream" % "2.5.21",
  "com.typesafe.akka" %% "akka-http" % "10.1.8",
  "com.typesafe.akka" %% "akka-actor" % "2.5.21",
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.0.1",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",
  "net.liftweb" %% "lift-json" % "3.3.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0"
)