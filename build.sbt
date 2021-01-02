
name := "sss-db"

version := "0.9.43-SNAPSHOT"

val scala213 = "2.13.2"

scalaVersion := "2.13.4"

updateOptions := updateOptions.value.withGigahorse(false)

parallelExecution in Test := false

//needed to retrieve ancillary, publish happens via global.sbt.
resolvers += "stepsoft" at "https://nexus.mcsherrylabs.com/repository/releases/"

resolvers += "stepsoft-snapshots" at "https://nexus.mcsherrylabs.com/repository/snapshots/"

dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

//libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.5.1" % Test

val excludeJetty = ExclusionRule(organization = "org.eclipse.jetty.aggregate")

libraryDependencies += "com.mcsherrylabs" %% "sss-ancillary" % "1.18" excludeAll(excludeJetty)

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.8.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.9.0"

libraryDependencies += "com.zaxxer" % "HikariCP" % "3.4.5"


