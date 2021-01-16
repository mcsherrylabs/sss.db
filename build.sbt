
name := "sss-db"

version := "0.9.47"

scalaVersion := "2.13.4"

updateOptions := updateOptions.value.withGigahorse(false)

parallelExecution in Test := false

//needed to retrieve ancillary, publish happens via global.sbt.
resolvers += "stepsoft" at "https://nexus.mcsherrylabs.com/repository/releases/"

resolvers += "stepsoft-snapshots" at "https://nexus.mcsherrylabs.com/repository/snapshots/"


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

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.5.1" % Test

val excludeJetty = ExclusionRule(organization = "org.eclipse.jetty.aggregate")

libraryDependencies += "com.mcsherrylabs" %% "sss-ancillary" % "1.18" excludeAll(excludeJetty)

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.8.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.9.0"

libraryDependencies += "com.zaxxer" % "HikariCP" % "3.4.5"


