name := "magi"
version := "1.2"
scalaVersion := "2.11.11"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0" % Provided

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.319"

// https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage
libraryDependencies += "com.microsoft.azure" % "azure-storage" % "7.0.0"
