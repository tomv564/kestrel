sbtResolver <<= (sbtResolver) { r =>
  Option(System.getenv("SBT_PROXY_REPO")) map { x =>
    Resolver.url("proxy repo for sbt", url(x))(Resolver.ivyStylePatterns)
  } getOrElse r
}

resolvers <<= (resolvers) { r =>
  (Option(System.getenv("SBT_PROXY_REPO")) map { url =>
    Seq("proxy-repo" at url)
  } getOrElse {
    r ++ Seq(
      "twitter.com" at "https://maven.twttr.com/",
      "scala-tools" at "http://scala-tools.org/repo-releases/",
      "maven" at "http://repo1.maven.org/maven2/",
      "freemarker" at "http://freemarker.sourceforge.net/maven2/"
    )
  }) ++ Seq("local" at ("file:" + System.getProperty("user.home") + "/.m2/repo/"))
}

externalResolvers <<= (resolvers) map identity

// addSbtPlugin("com.twitter" %% "sbt-package-dist" % "1.0.6")
// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
// addSbtPlugin("com.twitter" %% "sbt11-scrooge" % "3.0.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.6")

addSbtPlugin("com.twitter" %% "scrooge-sbt-plugin" % "4.5.0")