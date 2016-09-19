name := "parking_data"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
"org.apache.spark" %% "spark-sql"  % "1.4.1",
"com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3"
)
