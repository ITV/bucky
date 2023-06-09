import sbt.Attributed
import sbt.Keys.{publishArtifact, _}
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

name := "bucky"

lazy val scala212 = "2.12.18"
lazy val scala213 = "2.13.12"

scalaVersion := scala213
scalacOptions += "-Ypartial-unification"

val amqpClientVersion          = "5.20.0"
val scalaLoggingVersion        = "3.9.5"
val scalaTestVersion           = "3.2.17"
val argonautVersion            = "6.3.9"
val circeVersion               = "0.14.6"
val typeSafeVersion            = "1.4.3"
val catsEffectVersion          = "3.5.3"
val scalaXmlVersion            = "2.2.0"
val logbackVersion             = "1.4.14"
val catsEffectScalaTestVersion = "1.5.0"

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  setNextVersion,
  commitNextVersion,
  pushChanges
)
ThisBuild / publish / skip := true

releaseCrossBuild      := true
publishMavenStyle      := true
Test / publishArtifact := false
pomIncludeRepository := { _ =>
  false
}
releasePublishArtifactsAction := PgpKeys.publishSigned.value

pgpSigningKey := Some("C2B50980F625F2AF9D3CB3AA5709530EE8FA7576")

pgpPassphrase := Option(System.getenv("GPG_KEY_PASSPHRASE")).map(_.toArray)

lazy val kernelSettings = Seq(
  crossScalaVersions := Seq(scala212, scala213),
  scalaVersion       := scala213,
  organization       := "com.itv",
  scalacOptions ++= Seq("-feature", "-deprecation", "-Xfatal-warnings", "-language:higherKinds"),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishConfiguration      := publishConfiguration.value.withOverwrite(isSnapshot.value),
  publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(isSnapshot.value),
  publish / skip            := false,
  credentials ++= (for {
    username <- Option(System.getenv().get("SONATYPE_USER"))
    password <- Option(System.getenv().get("SONATYPE_PASS"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq,
  pomExtra :=
    <url>https://github.com/ITV/bucky</url>
      <licenses>
        <license>
          <name>ITV-OSS</name>
          <url>http://itv.com/itv-oss-licence-v1.0</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:ITV/bucky.git</url>
        <connection>scm:git@github.com:ITV/bucky.git</connection>
      </scm>
      <developers>
        <developer>
          <id>jfwilson</id>
          <name>Jamie Wilson</name>
          <url>https://github.com/jfwilson</url>
        </developer>
        <developer>
          <id>BeniVF</id>
          <name>Beni Villa Fernandez</name>
          <url>https://github.com/BeniVF</url>
        </developer>
        <developer>
          <id>leneghan</id>
          <name>Stuart Leneghan</name>
          <url>https://github.com/leneghan</url>
        </developer>
        <developer>
          <id>caoilte</id>
          <name>Caoilte O'Connor</name>
          <url>https://github.com/caoilte</url>
        </developer>
        <developer>
          <id>andrewgee</id>
          <name>Andrew Gee</name>
          <url>https://github.com/andrewgee</url>
        </developer>
        <developer>
          <id>smithleej</id>
          <name>Lee Smith</name>
          <url>https://github.com/smithleej</url>
        </developer>
        <developer>
          <id>sofiaaacole</id>
          <name>Sofia Cole</name>
          <url>https://github.com/sofiaaacole</url>
        </developer>
        <developer>
          <id>mcarolan</id>
          <name>Martin Carolan</name>
          <url>https://mcarolan.net/</url>
          <organization>ITV</organization>
          <organizationUrl>http://www.itv.com</organizationUrl>
        </developer>
        <developer>
          <id>cmcmteixeira</id>
          <name>Carlos Teixeira</name>
          <url>http://cmcmteixeira.github.io</url>
          <organization>ITV</organization>
          <organizationUrl>http://www.itv.com</organizationUrl>
        </developer>
      </developers>
)

lazy val core = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-core")
  .settings(kernelSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging"                 % scalaLoggingVersion,
      "org.scalatest"              %% "scalatest"                     % scalaTestVersion           % "test",
      "org.typelevel"              %% "cats-effect-testing-scalatest" % catsEffectScalaTestVersion % "test",
      "org.typelevel"              %% "cats-effect"                   % catsEffectVersion,
      "com.rabbitmq"                % "amqp-client"                   % amqpClientVersion,
      "ch.qos.logback"              % "logback-classic"               % logbackVersion             % "test",
      "org.scala-lang.modules"     %% "scala-collection-compat"       % "2.11.0"
    )
  )

lazy val test = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-test")
  .settings(kernelSettings)
  .dependsOn(core)
  .aggregate(core)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging"                 % scalaLoggingVersion,
      "org.scalatest"              %% "scalatest"                     % scalaTestVersion           % "test",
      "org.typelevel"              %% "cats-effect-testing-scalatest" % catsEffectScalaTestVersion % "test",
      "org.typelevel"              %% "cats-effect"                   % catsEffectVersion,
      "com.rabbitmq"                % "amqp-client"                   % amqpClientVersion,
      "ch.qos.logback"              % "logback-classic"               % logbackVersion             % "test"
    )
  )

lazy val it = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-it")
  .settings(kernelSettings)
  .dependsOn(core, test)
  .aggregate(core)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging"                 % scalaLoggingVersion,
      "org.scalatest"              %% "scalatest"                     % scalaTestVersion           % "test",
      "org.typelevel"              %% "cats-effect-testing-scalatest" % catsEffectScalaTestVersion % "test",
      "org.typelevel"              %% "cats-effect"                   % catsEffectVersion,
      "com.rabbitmq"                % "amqp-client"                   % amqpClientVersion,
      "com.typesafe"                % "config"                        % typeSafeVersion            % "test",
      "ch.qos.logback"              % "logback-classic"               % logbackVersion             % "test"
    )
  )

lazy val example = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-example")
  .settings(kernelSettings)
  .aggregate(core, argonaut, circe, test)
  .dependsOn(core, argonaut, circe, test)
  .settings(
    libraryDependencies ++= Seq(
      "io.argonaut"                %% "argonaut"                      % argonautVersion,
      "com.typesafe.scala-logging" %% "scala-logging"                 % scalaLoggingVersion,
      "org.scalatest"              %% "scalatest"                     % scalaTestVersion,
      "org.typelevel"              %% "cats-effect-testing-scalatest" % catsEffectScalaTestVersion % "test",
      "com.typesafe"                % "config"                        % typeSafeVersion,
      "dev.profunktor" %% "fs2-rabbit" % "5.0.0",
      "dev.profunktor" %% "fs2-rabbit-circe" % "5.0.0"
    )
  )

lazy val argonaut = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-argonaut")
  .settings(kernelSettings)
  .aggregate(core, test)
  .dependsOn(core, test % "test")
  .settings(
    libraryDependencies ++= Seq(
      "io.argonaut"                %% "argonaut"                      % argonautVersion,
      "com.typesafe.scala-logging" %% "scala-logging"                 % scalaLoggingVersion,
      "org.scalatest"              %% "scalatest"                     % scalaTestVersion           % "test",
      "org.typelevel"              %% "cats-effect-testing-scalatest" % catsEffectScalaTestVersion % "test"
    )
  )

lazy val circe = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-circe")
  .settings(kernelSettings)
  .aggregate(core, test)
  .dependsOn(core, test % "test")
  .settings(
    libraryDependencies ++= Seq(
      "io.circe"                   %% "circe-core"                    % circeVersion,
      "io.circe"                   %% "circe-generic"                 % circeVersion,
      "io.circe"                   %% "circe-parser"                  % circeVersion,
      "com.typesafe.scala-logging" %% "scala-logging"                 % scalaLoggingVersion,
      "org.scalatest"              %% "scalatest"                     % scalaTestVersion           % "test",
      "org.typelevel"              %% "cats-effect-testing-scalatest" % catsEffectScalaTestVersion % "test"
    )
  )

lazy val xml = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-xml")
  .settings(kernelSettings)
  .aggregate(core, test)
  .dependsOn(core, test % "test")
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules"     %% "scala-xml"                     % scalaXmlVersion,
      "com.typesafe.scala-logging" %% "scala-logging"                 % scalaLoggingVersion,
      "org.scalatest"              %% "scalatest"                     % scalaTestVersion           % "test",
      "org.typelevel"              %% "cats-effect-testing-scalatest" % catsEffectScalaTestVersion % "test"
    )
  )

lazy val root = (project in file("."))
  .aggregate(xml, circe, argonaut, example, test, core)
  .settings(publishArtifact := false)
