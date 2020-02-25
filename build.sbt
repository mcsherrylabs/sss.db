
name := "sss-db"

version := "0.9.40-SNAPSHOT"

val scala213 = "2.13.1"

scalaVersion := scala213

//Can't if using new 2.13 features.
//crossScalaVersions := Seq("2.12.10", scala213)

updateOptions := updateOptions.value.withGigahorse(false)

parallelExecution in Test := false

//needed to retrieve ancillary, publish happens via global.sbt.
resolvers += ("stepsoft" at "http://nexus.mcsherrylabs.com/repository/releases/").withAllowInsecureProtocol(true)

resolvers += ("stepsoft-snapshots" at "http://nexus.mcsherrylabs.com/repository/snapshots/").withAllowInsecureProtocol(true)

dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

//libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-M2" % Test

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.4.1" % Test

libraryDependencies += "com.mcsherrylabs" %% "sss-ancillary" % "1.10-SNAPSHOT"

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.2"

libraryDependencies += "com.zaxxer" % "HikariCP" % "2.4.7"


