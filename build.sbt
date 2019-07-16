
name := "sss-db"

version := "0.9.37"

scalaVersion := "2.12.6"

updateOptions := updateOptions.value.withGigahorse(false)

parallelExecution in Test := false

//needed to retrieve ancillary, publish happens via global.sbt.
resolvers += "stepsoft" at "http://nexus.mcsherrylabs.com/repository/releases/"

resolvers += "stepsoft-snapshots" at "http://nexus.mcsherrylabs.com/repository/snapshots/"

dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

//libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.4.1" % Test

libraryDependencies += "com.mcsherrylabs" %% "sss-ancillary" % "1.5-SNAPSHOT"

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.2"

libraryDependencies += "com.zaxxer" % "HikariCP" % "2.4.7"

libraryDependencies += "com.twitter" %% "util-collection" % "18.5.0"
