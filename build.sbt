lazy val commonSettings = Seq(
  organization := "com.icbc",
  version := "0.0.1",
  scalaVersion := "2.10.6",
  libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.3" % "test"
)

lazy val root = (project in file("."))
    .settings(commonSettings: _*)
    .settings(
      name := "pdlp",
      libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.11"
    )
