import sbt.Attributed
import sbt.Keys.{publishArtifact, _}
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

name := "bucky"

lazy val scala212 = "2.12.10"
lazy val scala213 = "2.13.2"
lazy val scala3   = "3.0.0-M3"

scalaVersion := scala212
scalacOptions += "-Ypartial-unification"

val amqpClientVersion   = "5.8.0"
val scalaLoggingVersion = "3.9.2"
val scalaTestVersion    = "3.2.3"
val argonautVersion     = "6.3.3"
val circeVersion        = "0.14.0-M3"
val typeSafeVersion     = "1.4.0"
val catsEffectVersion   = "2.3.1"
val scalaXmlVersion     = "2.0.0-M4"
val scalaz              = "7.2.22"
val logbackVersion      = "1.2.3"
val kamonVersion        = "2.1.3"
val log4catsVersion     = "1.2.0-RC2"

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
skip in publish in ThisBuild := true

releaseCrossBuild := true
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ =>
  false
}
releasePublishArtifactsAction := PgpKeys.publishSigned.value

pgpPublicRing := file("./ci/public.asc")

pgpSecretRing := file("./ci/private.asc")

pgpSigningKey := Some(-5373332187933973712L)

pgpPassphrase := Option(System.getenv("GPG_KEY_PASSPHRASE")).map(_.toArray)

useGpg := false

lazy val kernelSettings = Seq(
  crossScalaVersions := Seq(scala212, scala213, scala3),
  scalaVersion := scala212,
  organization := "com.itv",
  scalacOptions ++= Seq("-feature", "-deprecation", "-Xfatal-warnings"),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishConfiguration := publishConfiguration.value.withOverwrite(isSnapshot.value),
  publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(isSnapshot.value),
  skip in publish := false,
  credentials ++= (for {
    username <- Option(System.getenv().get("SONATYPE_USER"))
    password <- Option(System.getenv().get("SONATYPE_PASS"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq,
  pomExtra := (
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
)

lazy val core = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-core")
  .settings(kernelSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-xml" % scalaXmlVersion,
      "org.typelevel"          %% "log4cats-core"           % log4catsVersion, // Only if you want to Support Any Backend
      "org.typelevel"          %% "log4cats-slf4j"          % log4catsVersion, // Direct Slf4j Support - Recommended"
      "org.scalatest" %% "scalatest"      % scalaTestVersion % "test, it" excludeAll(ExclusionRule("org.scala-lang.modules")),
      "org.typelevel"          %% "cats-effect"             % catsEffectVersion,
      "com.rabbitmq"           % "amqp-client"              % amqpClientVersion,
      "ch.qos.logback"         % "logback-classic"          % logbackVersion % "test,it",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.0"
    )
  )
  .configs(IntegrationTest)

/**
  * CrossVersion.partialVersion(scalaVersion) match {
         case Some((2, scalaMajor)) if scalaMajor == 9 => Nil
         case _ => Seq("-language:_")
       }
  */
lazy val test = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-test")
  .settings(kernelSettings: _*)
  .dependsOn(core)
  .aggregate(core)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-xml" % scalaXmlVersion,
      "org.typelevel"  %% "log4cats-core"  % log4catsVersion, // Only if you want to Support Any Backend
      "org.typelevel"  %% "log4cats-slf4j" % log4catsVersion, // Direct Slf4j Support - Recommended"
      "org.scalatest" %% "scalatest"      % scalaTestVersion % "test, it" excludeAll(ExclusionRule("org.scala-lang.modules")),
      "org.typelevel"  %% "cats-effect"    % catsEffectVersion,
      "com.rabbitmq"   % "amqp-client"     % amqpClientVersion,
      "com.typesafe"   % "config"          % typeSafeVersion % "it",
      "ch.qos.logback" % "logback-classic" % logbackVersion % "test,it"
    )
  )

lazy val example = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-example")
  .settings(kernelSettings: _*)
  .aggregate(core, argonaut, circe, test)
  .dependsOn(core, argonaut, circe, test)
  .settings(
    libraryDependencies ++= Seq(
      "io.argonaut" %% "argonaut" % argonautVersion,
      "org.typelevel" %% "log4cats-core"  % log4catsVersion, // Only if you want to Support Any Backend
      "org.typelevel" %% "log4cats-slf4j" % log4catsVersion, // Direct Slf4j Support - Recommended"
      "org.scala-lang.modules" %% "scala-xml" % scalaXmlVersion,
      "org.scalatest" %% "scalatest"      % scalaTestVersion % "test" excludeAll(ExclusionRule("org.scala-lang.modules")),
      "com.typesafe"  % "config"          % typeSafeVersion
    )
  )

lazy val argonaut = project
  .settings(name := "com.itv")
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
      "org.typelevel" %% "log4cats-core"  % log4catsVersion, // Only if you want to Support Any Backend
      "org.typelevel" %% "log4cats-slf4j" % log4catsVersion, // Direct Slf4j Support - Recommended"
      "org.scalatest" %% "scalatest"      % scalaTestVersion % "test, it" excludeAll(ExclusionRule("org.scala-lang.modules"))
    )
  )

lazy val circe = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-circe")
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
      "io.circe" %% "circe-core"    % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser"  % circeVersion,
      "org.typelevel" %% "log4cats-core"  % log4catsVersion, // Only if you want to Support Any Backend
      "org.typelevel" %% "log4cats-slf4j" % log4catsVersion, // Direct Slf4j Support - Recommended"
      "org.scalatest" %% "scalatest"      % scalaTestVersion % "test, it" excludeAll(ExclusionRule("org.scala-lang.modules"))
    )
  )

lazy val kamon = project
  .settings(name := "com.itv")
  .settings(moduleName := "bucky-kamon")
  .settings(kernelSettings: _*)
  .aggregate(core, test)
  .dependsOn(core, test % "test,it")
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    internalDependencyClasspath in IntegrationTest += Attributed.blank((classDirectory in Test).value),
    parallelExecution in IntegrationTest := false,
    crossScalaVersions := Seq(scala212)
  )
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel"  %% "log4cats-core"  % log4catsVersion, // Only if you want to Support Any Backend
      "org.typelevel"  %% "log4cats-slf4j" % log4catsVersion, // Direct Slf4j Support - Recommended"
      "org.scalatest" %% "scalatest"      % scalaTestVersion % "test, it" excludeAll(ExclusionRule("org.scala-lang.modules")),
      "io.kamon"       %% "kamon-bundle"   % kamonVersion,
      "io.kamon"       %% "kamon-testkit"  % kamonVersion % "test",
      "ch.qos.logback" % "logback-classic" % logbackVersion % "test, it"
    )
  )

lazy val xml = project
  .settings(name := "com.itv")
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
      "org.scala-lang.modules" %% "scala-xml" % scalaXmlVersion,
      "org.typelevel" %% "log4cats-core"  % log4catsVersion, // Only if you want to Support Any Backend
      "org.typelevel" %% "log4cats-slf4j" % log4catsVersion, // Direct Slf4j Support - Recommended"
      "org.scalatest" %% "scalatest"      % scalaTestVersion % "test, it" excludeAll(ExclusionRule("org.scala-lang.modules"))
    )
  )

lazy val root = (project in file("."))
  .aggregate(xml, circe, argonaut, example, test, core, kamon)
  .settings(publishArtifact := false)
