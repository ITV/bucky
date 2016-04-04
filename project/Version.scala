import sbt.Keys._

object Version extends sbt.Build {

  val majorVersion = "0.0"

  def buildNumber = {
    scala.util.Properties.envOrElse("BUILD_NUMBER", "SNAPSHOT")
  }

  val versionSetting = version := majorVersion + "." + buildNumber

  override lazy val settings = super.settings :+ versionSetting
}
