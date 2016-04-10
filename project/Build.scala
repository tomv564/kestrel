import sbt._
import Keys._
// import com.twitter.sbt._
// import sbtassembly.AssemblyPlugin.autoImport._
import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.SbtNativePackager.Universal
import com.typesafe.sbt.packager.docker.DockerPlugin
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging

object Kestrel extends Build {
  val finagleVersion = "6.34.0"

  lazy val root = Project(
    id = "kestrel",
    base = file("."),
    settings = Seq(
      name := "kestrel",
      organization := "net.lag",
      version := "2.4.8-SNAPSHOT",
      scalaVersion := "2.11.7",

      // time-based tests cannot be run in parallel
      logBuffered in Test := false,
      parallelExecution in Test := false,

      resolvers += "twitter.com" at "https://maven.twttr.com/",

      libraryDependencies ++= Seq(
        "com.twitter" %% "ostrich" % "9.17.0",
        // "com.twitter" %% "naggati" % "4.1.0",
        "com.twitter" %% "finagle-core" % finagleVersion,
        "com.twitter" %% "finagle-ostrich4" % finagleVersion,
        "com.twitter" %% "finagle-thrift" % finagleVersion, // override scrooge's version
        "com.twitter" %% "finagle-memcached" % finagleVersion exclude("org.slf4j", "slf4j-jdk14"), // override scrooge's version
        "com.twitter" %% "scrooge-core" % "4.6.0",
        // "com.twitter.common.zookeeper" % "server-set" % "1",
        // for tests only:
        "junit" % "junit" % "4.10" % "test",
        "org.mockito" % "mockito-all" % "1.9.5" % "test",
        "org.specs2" %% "specs2" % "3.7" % "test",
        "org.jmock" % "jmock" % "2.4.0" % "test",
        "cglib" % "cglib" % "2.1_3" % "test",
        "asm" % "asm" % "1.5.3" % "test",
        "org.objenesis" % "objenesis" % "1.1" % "test",
        "org.hamcrest" % "hamcrest-all" % "1.1" % "test",
        "org.scalatest" %% "scalatest" % "2.2.6" % "test"
      ),

      mainClass in Compile := Some("net.lag.kestrel.Kestrel"),

      javaOptions in Universal ++= Seq(
        // -J params will be added as jvm parameters
        "-J-Xmx1024m",

        // others will be added as app parameters
        "-Dstage=production"
      ),

      dockerExposedPorts := Seq(22133, 2229, 2223),
      dockerUpdateLatest := true

      // test in assembly := {},

      // assemblyMergeStrategy in assembly := {
      //   case "com/twitter/common/args/apt/cmdline.arg.info.txt.1"  => MergeStrategy.discard
      //   case x =>
      //     val oldStrategy = (assemblyMergeStrategy in assembly).value
      //     oldStrategy(x)
      // }
      // CompileThriftScrooge.scroogeVersion := "3.0.1",
      // PackageDist.packageDistConfigFilesValidationRegex := Some(".*"),
      // SubversionPublisher.subversionRepository := Some("https://svn.twitter.biz/maven-public"),
      // publishArtifact in Test := true
    )
  ).
  enablePlugins(JavaAppPackaging).
  enablePlugins(DockerPlugin)
}
