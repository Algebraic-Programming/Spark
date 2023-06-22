

val shared = Seq(
	organization := "com.huawei",
	version      := "0.1.0",
	scalaVersion := "2.13.11",
	Compile/packageBin/artifactPath := baseDirectory.value / ".." / "build" / (name.value + ".jar")
)

val source: File = file(".")

lazy val graphBLAS = (project in file("graphBLAS"))
	.settings(
		shared,
		name := "graphBLAS",
		libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0",
	)

lazy val examples = (project in file("examples"))
	.settings(
		shared,
		name := "examples",
		libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0",
  	)
	.dependsOn(graphBLAS)

