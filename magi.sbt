name := "Cloud Management Kits"
version := "1.0"
scalaVersion := "2.11.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.1.0"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.84"

// https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage
libraryDependencies += "com.microsoft.azure" % "azure-storage" % "5.0.0"
