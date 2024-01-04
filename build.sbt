
// scalacOptions += "-deprecation"

val shared = Seq(
	organization := "com.huawei",
	version      := "0.1.0",
	scalaVersion := "2.13.11",
	libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0" % "provided",
	libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.4.0" % "provided",
	scalacOptions ++= Seq("-deprecation", "-feature"),
	Compile/packageBin/artifactPath := baseDirectory.value / ".." / "build" / (name.value + ".jar"),
	assemblyPackageScala / assembleArtifact := false,
	assembly / assemblyOutputPath := file( s"${baseDirectory.value}/../build/${(assembly/assemblyJarName).value}" )
)

val source: File = file(".")

lazy val graphBLAS = (project in file("graphBLAS"))
	.settings(
		shared,
		name := "graphBLAS"
	)

lazy val examples = (project in file("examples"))
	.settings(
		shared,
		name := "examples",
		libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"
  	)
	.dependsOn(graphBLAS)

