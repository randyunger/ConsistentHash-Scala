name := "ConsistentHash"

organization := "com.ungersoft"

version := "0.0.1"

scalaVersion := "2.10.0"

initialCommands in console := "import com.ungersoft.consistenthash._"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "1.9.1" % "test"
)

resolvers ++= Seq(
	"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)
