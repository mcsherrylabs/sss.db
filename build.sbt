
scalaVersion := "2.11.4"

//scalaVersion := "2.10.4"

name := "sss-db"

version := "0.9"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.3.2"

libraryDependencies += "mcsherrylabs.com" %% "sss-ancillary" % "0.9"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.0"


