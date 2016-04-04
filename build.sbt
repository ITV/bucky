name := "bucky"

organization := "itv"

lazy val root = project.in(file(".")).configs(IntegrationTest)

Defaults.itSettings

parallelExecution in IntegrationTest := false

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-feature", "-deprecation", "-Xfatal-warnings")

internalDependencyClasspath in IntegrationTest += Attributed.blank((classDirectory in Test).value)

val contentDeliverySharedVersion = "1.0-591"
val commonPlatformServicesSharedVersion = "37.4.0"
val amqpClientVersion = "3.3.1"
val scalaLoggingVersion = "3.1.0"
val scalaTestVersion = "2.2.1"
val mockitoVersion = "1.9.0"

libraryDependencies ++= Seq(
  "com.rabbitmq" % "amqp-client" % amqpClientVersion,
  "itv.contentdelivery" %% "contentdelivery-shared-lifecycle" % contentDeliverySharedVersion,
  "itv.contentdelivery" %% "contentdelivery-shared-httpyroraptor" % contentDeliverySharedVersion % "test,it",
  "itv.contentdelivery" %% "contentdelivery-shared-test-utilities" % contentDeliverySharedVersion % "test,it",
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  "itv.cps" %% "cps-utils" % commonPlatformServicesSharedVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test,it",
  "org.mockito" % "mockito-core" % mockitoVersion)

//grab some dependencies from on-premise artifactory for now, until they have been migrated over to artifactory-online
resolvers += "ITV Libraries" at "http://cpp-artifactory.cpp.o.itv.net.uk:8081/artifactory/libs-release-local/"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo := Some("Artifactory Realm" at "https://itvrepos.artifactoryonline.com/itvrepos/cd-scala-libs")
