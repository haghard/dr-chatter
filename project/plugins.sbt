
resolvers += Classpaths.typesafeReleases

addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")
addSbtPlugin("com.scalapenos" % "sbt-prompt"       % "1.0.2")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.0")

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)