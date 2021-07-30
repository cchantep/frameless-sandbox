import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin

object Compiler extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = JvmPlugin

  val scala212 = "2.12.14"
  val scala213 = "2.13.6"

  override def projectSettings = Seq(
    scalaVersion := scala213,
    scalacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-explaintypes",
      "-unchecked",
      "-deprecation",
      "-feature",
      //"-language:higherKinds",
      "-Xlint",
      "-g:vars"
    ),
    scalacOptions ++= {
      if (scalaBinaryVersion.value == "2.13") {
        Seq(
          "-Werror",
          "-Wnumeric-widen",
          "-Wdead-code",
          "-Wvalue-discard",
          "-Wunused",
          "-Wmacros:after",
          "-Woctal-literal",
          "-Wextra-implicit")

      } else {
        Seq(
          "-Xfatal-warnings",
          "-Ywarn-numeric-widen",
          "-Ywarn-dead-code",
          "-Ywarn-value-discard",
          "-Ywarn-infer-any",
          "-Ywarn-unused",
          "-Ywarn-unused-import",
          "-Ywarn-macros:after")
      }
    },
    Test / scalacOptions ~= {
      _.filterNot(_ == "-Werror")
    },
    Compile / console / scalacOptions ~= {
      _.filterNot { opt =>
        opt.startsWith("-P") || opt.startsWith("-X") || opt.startsWith("-W")
      }
    },
    Test / console / scalacOptions ~= {
      _.filterNot { opt =>
        opt.startsWith("-P") || opt.startsWith("-X") || opt.startsWith("-W")
      }
    }
  )
}
