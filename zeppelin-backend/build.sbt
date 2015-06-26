
resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

javacOptions ++= Seq(
  "-source", "1.6",
  "-Xlint:-options" // Suppress Java 6 necessary warning
)

crossPaths := false

autoScalaLibrary := false

libraryDependencies ++= Seq(
  "org.apache.zeppelin" % "zeppelin-interpreter" % "0.6.0-incubating-SNAPSHOT"
)