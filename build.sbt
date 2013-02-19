name := "akka-fuzz"

organization := "com.ungersoft"

version := "0.0.1"

scalaVersion := "2.10.0"

libraryDependencies ++= Seq(
   "com.typesafe.akka" %% "akka-actor" % "2.1.0"
  ,"com.typesafe" % "config" % "0.4.0"
  ,"com.typesafe.akka" %% "akka-testkit" % "2.1.0"
  ,"org.scalatest" %% "scalatest" % "1.9.1" % "test"
  ,"org.specs2" %% "specs2" % "1.13" % "test"
  ,"org.mockito" % "mockito-all" % "1.9.5" % "test"
)

resolvers ++= Seq(
	"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)
