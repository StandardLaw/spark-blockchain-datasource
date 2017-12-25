// Every once in a while run `sbt dependencyUpdates` and `sbt dependencyCheckAggregate` here
import Tests._
import sbt.librarymanagement.Resolver

enablePlugins(GitVersioning)
git.useGitDescribe := true

val sparkVersion = "2.2.0"
val sparkScalaVersion = "2.11.8" // Spark relies on a specific version of Scala (including for some hacks)

lazy val defaultSettings = Seq(
  organization := "com.liorregev",
  scalaVersion := sparkScalaVersion,

  javaOptions ++= Seq("-Xms512M", "-Xmx8192M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),

  scalacOptions ++= Seq(
    "-feature", "-deprecation", "-unchecked", "-explaintypes",
    "-encoding", "UTF-8", // yes, this is 2 args
    "-language:reflectiveCalls", "-language:implicitConversions", "-language:postfixOps", "-language:existentials",
    "-language:higherKinds",
    // http://blog.threatstack.com/useful-scala-compiler-options-part-3-linting
    "-Xcheckinit", "-Xexperimental", "-Xfatal-warnings", /*"-Xlog-implicits", */"-Xfuture", "-Xlint",
    "-Ywarn-dead-code", "-Ywarn-inaccessible", "-Ywarn-numeric-widen", "-Yno-adapted-args", "-Ywarn-unused-import",
    "-Ywarn-unused"
  ),

  wartremoverErrors ++= Seq(
    Wart.StringPlusAny, Wart.FinalCaseClass, Wart.JavaConversions, Wart.Null, Wart.Product, Wart.Serializable,
    Wart.LeakingSealed, Wart.While, Wart.Return, Wart.ExplicitImplicitTypes, Wart.Enumeration, Wart.FinalVal,
    Wart.TryPartial, Wart.TraversableOps, Wart.OptionPartial, ContribWart.SomeApply
  ),

  wartremoverWarnings ++= wartremover.Warts.allBut(
    Wart.Nothing, Wart.DefaultArguments, Wart.Throw, Wart.MutableDataStructures, Wart.NonUnitStatements, Wart.Overloading,
    Wart.Option2Iterable, Wart.ImplicitConversion, Wart.ImplicitParameter, Wart.Recursion,
    Wart.Any, Wart.Equals, // Too many warnings because of spark's Row
    Wart.AsInstanceOf, // Too many warnings because of bad DI practices
    Wart.ArrayEquals // Too many warnings because we're using byte arrays in Spark
  ),

  testFrameworks := Seq(TestFrameworks.ScalaTest),
  logBuffered in Test := false,

  scalaVersion := sparkScalaVersion,

  resolvers ++= Seq(
    Resolver.mavenLocal,
    Resolver.sonatypeRepo("public"),
    Resolver.typesafeRepo("releases"),
    "ethereum" at "https://dl.bintray.com/ethereum/maven/",
    "jitpack" at "https://jitpack.io"
  ),

  // This needs to be here for Coursier to be able to resolve the "tests" classifier, otherwise the classifier's ignored
  classpathTypes += "test-jar",

  libraryDependencies ++= Seq(
    "org.apache.spark"             %% "spark-core"                   % sparkVersion    % "provided,test",
    "org.apache.spark"             %% "spark-sql"                    % sparkVersion    % "provided,test",
    "org.apache.spark"             %% "spark-hive"                   % sparkVersion    % "provided,test",
    "org.apache.spark"             %% "spark-catalyst"               % sparkVersion    % "provided,test",

    "com.google.guava"             %  "guava"                        % "14.0.1"        % "provided,test",
    "org.ethereum"                 %  "ethereumj-core"               % "1.6.3-RELEASE" exclude("com.google.guava", "guava"),
    "org.bitcoinj"                 %  "bitcoinj-core"                % "0.14.5"        exclude("com.google.guava", "guava"),
    "com.fasterxml.jackson.module" %  "jackson-module-paranamer"     % "2.8.5",
    "com.fasterxml.jackson.module" %% "jackson-module-scala"         % "2.8.5",
    "org.web3j"                    %  "core"                         % "3.1.1",

    "org.apache.httpcomponents"    %  "httpclient"                   % "4.5.4"         % "test" classifier "tests",
    "junit"                        %  "junit"                        % "4.12"          % "test",
    "org.joda"                     %  "joda-convert"                 % "1.9.2"         % "test",
    "com.typesafe.play"            %% "play-json"                    % "2.6.8"         % "test",
    "org.scalatest"                %% "scalatest"                    % "2.2.6"         % "test"
  ),

  dependencyOverrides ++= Seq(
    "com.google.guava"    %  "guava"                        % "14.0.1"
  )
)

lazy val assemblySettings = Seq(
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  test in assembly := {},

  assemblyMergeStrategy in assembly := {
    case x if x.endsWith("application.conf") => MergeStrategy.first
    case x if x.endsWith(".class") => MergeStrategy.last
    case x if x.endsWith(".properties") => MergeStrategy.last
    case x if x.contains("/resources/") => MergeStrategy.last
    case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
    case x if x.startsWith("META-INF/mimetypes.default") => MergeStrategy.first
    case x if x.startsWith("META-INF/maven/org.slf4j/slf4j-api/pom.") => MergeStrategy.first
    case x if x.startsWith("CHANGELOG.") => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      if (oldStrategy == MergeStrategy.deduplicate)
        MergeStrategy.first
      else
        oldStrategy(x)
  }
)

lazy val root = project.in(file("."))
  .settings(defaultSettings ++ assemblySettings)
  .settings(
    // Allow parallel execution of tests as long as each of them gets its own JVM to create a SparkContext on (see SPARK-2243)
    fork in Test := true,
    testGrouping in Test := (definedTests in Test)
      .value
      .map(test => Group(test.name, Seq(test), SubProcess(ForkOptions())))
  )