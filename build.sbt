
name := "sss-db"

version := "0.9.58"

scalaVersion := "2.13.16"

publishMavenStyle := true

pomIncludeRepository := { _ => false }

updateOptions := updateOptions.value.withGigahorse(false)

organization := "com.mcsherrylabs"

// Sonatype Central Portal (replaced legacy OSSRH in Feb 2024)
sonatypeCredentialHost := "central.sonatype.com"

publishTo := sonatypePublishToBundle.value

credentials ++= Seq(
  Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
) ++ sys.env.get("SONATYPE_USERNAME").map(u =>
  Credentials("Sonatype Nexus Repository Manager", "central.sonatype.com", u,
    sys.env.getOrElse("SONATYPE_PASSWORD", ""))
).toSeq

dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

// Tests intentionally exercise the deprecated row[T](col) API — suppress the warnings
Test / scalacOptions += "-Wconf:cat=deprecation:s"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.6.1" % Test

val excludeJetty = ExclusionRule(organization = "org.eclipse.jetty.aggregate")

libraryDependencies += "com.mcsherrylabs" %% "sss-ancillary" % "1.26" excludeAll(excludeJetty)

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.9.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.11.1"

val excludeSlf4j = ExclusionRule(organization = "org.slf4j")

libraryDependencies += "com.zaxxer" % "HikariCP" % "5.0.1" excludeAll(excludeSlf4j)

usePgpKeyHex("323F3F6EBDB010C1265C69F040DD84EA50085F5D")

javacOptions := Seq("--release", "11")

// ScalaDoc configuration
Compile / doc / scalacOptions ++= Seq(
  "-groups",                           // Group related APIs
  "-implicits",                        // Document implicit conversions
  "-diagrams",                         // Generate inheritance diagrams
  "-doc-title", "sss.db",
  "-doc-version", version.value,
  "-doc-root-content", "docs/scaladoc-root.txt"
)

// Output directory for ScalaDoc
Compile / doc / target := file("docs/api/scaladoc")

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
