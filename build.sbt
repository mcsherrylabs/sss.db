
name := "sss-db"

version := "0.9.54"

scalaVersion := "2.13.8"

updateOptions := updateOptions.value.withGigahorse(false)

//needed to retrieve ancillary, publish happens via global.sbt.
resolvers += "stepsoft" at "https://nexus.mcsherrylabs.com/repository/releases/"

resolvers += "stepsoft-snapshots" at "https://nexus.mcsherrylabs.com/repository/snapshots/"

organization := "com.mcsherrylabs"

publishTo := {
  val nexus = "https://nexus.mcsherrylabs.com/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "repository/snapshots")
  else
    Some("releases"  at nexus + "repository/releases")
}

credentials += sys.env.get("NEXUS_USER").map(userName => Credentials(
  "Sonatype Nexus Repository Manager",
  "nexus.mcsherrylabs.com",
  userName,
  sys.env.getOrElse("NEXUS_PASS", ""))
).getOrElse(
  Credentials(Path.userHome / ".ivy2" / ".credentials")
)


dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

//libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.6.1" % Test

val excludeJetty = ExclusionRule(organization = "org.eclipse.jetty.aggregate")

libraryDependencies += "com.mcsherrylabs" %% "sss-ancillary" % "1.22" excludeAll(excludeJetty)

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.9.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.11.1"

val excludeSlf4j = ExclusionRule(organization = "org.slf4j")

libraryDependencies += "com.zaxxer" % "HikariCP" % "5.0.1" excludeAll(excludeSlf4j)


