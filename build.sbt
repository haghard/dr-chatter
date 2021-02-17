import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbt.CrossVersion

val akkaVersion = "2.5.32"

val `dr-chatter` = project
  .in(file("."))
  .settings(SbtMultiJvm.multiJvmSettings: _*)
  .settings(
    name := "dr-chatter",
    version := "0.0.1",
    scalaVersion :=  "2.13.2", //"2.12.11",

    //scalacOptions in Compile ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),

    javacOptions in Compile ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),

    //javaOptions in run ++= Seq("-Xms128m", "-Xmx1024m"),

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-distributed-data" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion,

      //"com.github.TanUkkii007" %% "akka-cluster-custom-downing" % "0.0.12",
      "org.sisioh"        %% "akka-cluster-custom-downing" % "0.1.0",

      "com.typesafe.akka" %% "akka-http" % "10.2.3",
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,

      "org.rocksdb" % "rocksdbjni" %  "5.17.2",
      "com.h2database"  % "h2"      %  "1.4.199",

      "ch.qos.logback" % "logback-classic" % "1.2.3",

      //"com.rbmhtechnology" %% "eventuate-crdt" % "0.10",

      "org.hdrhistogram"  % "HdrHistogram" %  "2.1.10",
      ("com.lihaoyi" % "ammonite" % "2.3.8-32-64308dc3" % "test").cross(CrossVersion.full),

      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion),

    //
    fork in run := true,

    // disable parallel tests
    parallelExecution in Test := false,

    javaOptions ++= Seq("-Xmx4G", "-XX:MaxMetaspaceSize=3G", "-XX:+UseG1GC")

  ) configs MultiJvm

//https://tpolecat.github.io/2017/04/25/scalac-flags.html

scalafmtOnCompile := true

//test:run test:console
sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue

promptTheme := ScalapenosTheme

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"