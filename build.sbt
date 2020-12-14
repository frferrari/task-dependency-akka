name := "nexthink-challenge"

version := "1.0"

scalaVersion := "2.13.1"

lazy val akkaVersion = "2.6.10"
lazy val akkaHttpVersion = "10.2.2"

resolvers += Resolver.mavenLocal

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

logBuffered in Test := false

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,

  "org.scala-graph" %% "graph-core" % "1.13.2" withSources(),

  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.1.4" % Test
)
