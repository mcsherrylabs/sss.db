
name := "sss-db"

version := "0.9.33"

scalaVersion := "2.11.8"

libraryDependencies += "joda-time" % "joda-time" % "2.8.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.3.4" % Test

libraryDependencies += "com.mcsherrylabs" % "sss-ancillary_2.11" % "1.0"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.0"

libraryDependencies += "com.zaxxer" % "HikariCP" % "2.4.7"

libraryDependencies += "com.twitter" %% "util-collection" % "6.27.0"
