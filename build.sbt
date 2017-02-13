import sbt.Attributed
import sbt.Keys._

name := "bucky"

organization := "itv"
crossScalaVersions := Seq("2.11.8", "2.12.1")

val itvLifecycleVersion = "0.16"
val amqpClientVersion = "4.0.2"
val scalaLoggingVersion = "3.5.0"
val scalaTestVersion = "3.0.1"
val mockitoVersion = "1.9.0"
val argonautVersion = "6.2-RC2"

lazy val kernelSettings = Seq(
  scalaVersion := "2.12.1",
  scalacOptions ++= Seq("-feature", "-deprecation", "-Xfatal-warnings"),
  credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
  publishTo in ThisBuild := Some("Artifactory Realm" at "https://itvrepos.artifactoryonline.com/itvrepos/cd-scala-libs")
)

lazy val core = project
  .settings(name := "itv")
  .settings(moduleName := "bucky-core")
  .settings(kernelSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "com.itv" %% "lifecycle" % itvLifecycleVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.mockito" % "mockito-core" % mockitoVersion % "test"
    )
  )
  .configs(IntegrationTest)

lazy val test = project
  .settings(name := "itv")
  .settings(moduleName := "bucky-test")
  .settings(kernelSettings: _*)
  .aggregate(core)
  .dependsOn(core)
  .settings(
    libraryDependencies ++= Seq(
      "com.itv" %% "lifecycle" % itvLifecycleVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "org.apache.qpid" % "qpid-broker" % "6.0.4",
      "org.scalatest" %% "scalatest" % scalaTestVersion
    )
  )

lazy val example = project
  .settings(name := "itv")
  .settings(moduleName := "bucky-example")
  .settings(kernelSettings: _*)
  .aggregate(core, rabbitmq, argonaut)
  .dependsOn(core, rabbitmq, argonaut)
  .settings(
    libraryDependencies ++= Seq(
      "io.argonaut" %% "argonaut" % argonautVersion,
      "com.itv" %% "lifecycle" % itvLifecycleVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "org.apache.qpid" % "qpid-broker" % "6.0.4",
      "org.scalatest" %% "scalatest" % scalaTestVersion
    )
  )

lazy val argonaut = project
  .settings(name := "itv")
  .settings(moduleName := "bucky-argonaut")
  .settings(kernelSettings: _*)
  .aggregate(core, test)
  .dependsOn(core, test % "test,it")
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    internalDependencyClasspath in IntegrationTest += Attributed.blank((classDirectory in Test).value),
    parallelExecution in IntegrationTest := false
  )
  .settings(
    libraryDependencies ++= Seq(
      "io.argonaut" %% "argonaut" % argonautVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test, it"
    )
  )


lazy val xml = project
  .settings(name := "itv")
  .settings(moduleName := "bucky-xml")
  .settings(kernelSettings: _*)
  .aggregate(core, test)
  .dependsOn(core, test % "test,it")
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    internalDependencyClasspath in IntegrationTest += Attributed.blank((classDirectory in Test).value),
    parallelExecution in IntegrationTest := false
  )
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test, it"
    )
  )


lazy val rabbitmq = project
  .settings(name := "itv")
  .settings(moduleName := "bucky-rabbitmq")
  .settings(kernelSettings: _*)
  .aggregate(core, test)
  .dependsOn(core, test % "test,it")
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    internalDependencyClasspath in IntegrationTest += Attributed.blank((classDirectory in Test).value),
    parallelExecution in IntegrationTest := false
  )
  .settings(
    libraryDependencies ++= Seq(
      "com.itv" %% "lifecycle" % itvLifecycleVersion,
      "com.rabbitmq" % "amqp-client" % amqpClientVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test, it",
      "io.netty" % "netty" % "3.4.2.Final" % "test,it",
      "com.typesafe" % "config" % "1.2.1" % "it"
    )
  )
