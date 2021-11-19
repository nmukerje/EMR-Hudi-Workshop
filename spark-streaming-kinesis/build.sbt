name := "Spark-Structured-Streaming-Kinesis-Hudi"

version := "1.0"
scalaVersion := "2.11.12"
val sparkVersion = "2.4.5"
val scala_tool_version="2.11"

libraryDependencies += "log4j" % "log4j" % "1.2.14"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion


libraryDependencies += "org.apache.spark" % "spark-streaming-kinesis-asl_2.11" % sparkVersion
libraryDependencies += "com.qubole.spark" % "spark-sql-kinesis_2.11" % "1.2.0_spark-2.4"
libraryDependencies += "org.apache.hudi" % "hudi-spark-bundle_2.11" % "0.9.0"

fork in run := true

libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "6.3.0"
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
    val cp = (fullClasspath in assembly).value
    cp filter { f =>
      f.data.getName.contains("spark-core") ||
      f.data.getName.contains("hudi-spark-bundle_2.11")
    }
  }