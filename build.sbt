
name := "sss-db"

version := "0.9.30"


resolvers += "stepsoft" at "http://nexus.mcsherrylabs.com/nexus/content/groups/public"

libraryDependencies += "joda-time" % "joda-time" % "2.8.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.3.2"

libraryDependencies += "mcsherrylabs.com" %% "sss-ancillary" % "0.9.4"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.0"

libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.0"

libraryDependencies += "com.zaxxer" % "HikariCP" % "2.4.5"

libraryDependencies += "com.twitter" %% "util-collection" % "6.27.0"
