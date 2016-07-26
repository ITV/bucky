import sbt.Attributed
import sbt.Keys._

name := "bucky"

organization := "itv"

val contentDeliverySharedVersion = "1.0-591"
val amqpClientVersion = "3.3.1"
val scalaLoggingVersion = "3.1.0"
val scalaTestVersion = "2.2.1"
val mockitoVersion = "1.9.0"

lazy val kernelSettings = Seq(
  scalaVersion := "2.11.8",
  scalacOptions ++= Seq("-feature", "-deprecation", "-Xfatal-warnings"),
  //grab some dependencies from on-premise artifactory for now, until they have been migrated over to artifactory-online
  resolvers += "ITV Libraries" at "http://cpp-artifactory.cpp.o.itv.net.uk:8081/artifactory/libs-release-local/",
  credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
  publishTo in ThisBuild := Some("Artifactory Realm" at "https://itvrepos.artifactoryonline.com/itvrepos/cd-scala-libs")
)

lazy val core = project
  .settings(name := "itv")
  .settings(moduleName := "bucky-core")
  .settings(kernelSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "itv.contentdelivery" %% "contentdelivery-shared-lifecycle" % contentDeliverySharedVersion,
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
      "itv.contentdelivery" %% "contentdelivery-shared-lifecycle" % contentDeliverySharedVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "org.apache.qpid" % "qpid-broker" % "6.0.4",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
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
      "com.rabbitmq" % "amqp-client" % amqpClientVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      //      "org.slf4j" % "slf4j-api" % "1.6.1",
      "itv.contentdelivery" %% "contentdelivery-shared-lifecycle" % contentDeliverySharedVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test, it",
      "itv.contentdelivery" %% "contentdelivery-shared-test-utilities" % contentDeliverySharedVersion % "test,it" excludeAll ExclusionRule(organization = "com.google.guava")
      //      "io.netty" % "netty-all" % "4.1.2.Final" % "test,it",
      //      "com.google.guava" % "guava" % "18.0" % "it"
    )
  )
