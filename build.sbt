val scala3Version = "3.8.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "Disneylandproject",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,
    libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    libraryDependencies += "com.lihaoyi" %% "upickle" % "3.1.0"
  )
