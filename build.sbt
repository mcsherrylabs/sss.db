
name := "sss-db"

version := "0.9.35"

scalaVersion := "2.12.6"

updateOptions := updateOptions.value.withGigahorse(false)

parallelExecution in Test := false

resolvers += "stepsoft" at "http://nexus.mcsherrylabs.com/repository/releases/"

dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

libraryDependencies += "joda-time" % "joda-time" % "2.8.2"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.4.1" % Test

libraryDependencies += "com.mcsherrylabs" %% "sss-ancillary" % "1.3"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.0"

libraryDependencies += "com.zaxxer" % "HikariCP" % "2.4.7"

libraryDependencies += "com.twitter" %% "util-collection" % "18.5.0"
