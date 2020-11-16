lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "org.sasdutta",
      scalaVersion := "2.11.10"
    )),
    name := "spark-deequ-poc",
    version := "0.0.1"

  )


libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"
libraryDependencies += "com.amazon.deequ" % "deequ" % "1.0.5"

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
