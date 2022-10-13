
name := "sss-db"

version := "0.9.57"

scalaVersion := "2.13.10"

publishMavenStyle := true

pomIncludeRepository := { _ => false }

updateOptions := updateOptions.value.withGigahorse(false)

organization := "com.mcsherrylabs"

publishTo := Some {
  val sonaUrl = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    "snapshots" at sonaUrl + "content/repositories/snapshots"
  else
    "releases" at sonaUrl + "service/local/staging/deploy/maven2"
}

credentials += sys.env.get("SONA_USER").map(userName => Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  userName,
  sys.env.getOrElse("SONA_PASS", ""))
).getOrElse(
  Credentials(Path.userHome / ".ivy2" / ".credentials")
)

dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.6.1" % Test

val excludeJetty = ExclusionRule(organization = "org.eclipse.jetty.aggregate")

libraryDependencies += "com.mcsherrylabs" %% "sss-ancillary" % "1.25" excludeAll(excludeJetty)

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.9.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.11.1"

val excludeSlf4j = ExclusionRule(organization = "org.slf4j")

libraryDependencies += "com.zaxxer" % "HikariCP" % "5.0.1" excludeAll(excludeSlf4j)

usePgpKeyHex("F4ED23D42A612E27F11A6B5AF75482A04B0D9486")

javacOptions := Seq("-source", "11", "-target", "11")

pomExtra := (
  <url>https://github.com/mcsherrylabs/sss.db</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:mcsherrylabs/sss.db.git</url>
      <connection>scm:git:git@github.com:mcsherrylabs/sss.db.git</connection>
    </scm>
    <developers>
      <developer>
        <id>mcsherrylabs</id>
        <name>Alan McSherry</name>
        <url>http://mcsherrylabs.com</url>
      </developer>
    </developers>)
