# magi - cloud storage MAnaGement kIt #

This software provides several functions for managing 
large-scale cloud object store, leveraging Apache Spark. It uses AWS Java SDK and Azure
Storage Java SDK to communicate with Amazon AWS, Microsoft Azure or NetApp StorageGrid.

One specific use case for NetApp StorageGrid customers is to use this software
to replicate or migrate objects between a StorageGrid system and Microsoft Azure or Amazon S3.

## Functions Implemented ##
* Bucket synchronization  
	Replicate or migrate objects between two buckets in AWS, Azure or NetApp StorageGrid  
* Empty a bucket  
	Delete all objects in a bucket  
* List a bucket  
	List all objects in a bucket  
* Fill a bucket  
	Populate a bucket with objects 

## Software ##
* Spark-2.1.0-bin-hadoop2.7
* aws-java-sdk-1.11.84.jar  
	Available at: https://github.com/NTAP/magi/releases/download/1.0/aws-java-sdk-1.11.84.jar  
	More recent versions are available at: https://aws.amazon.com/sdk-for-java/
* azure-storage-5.0.0.jar  
	Available at: https://github.com/NTAP/magi/releases/download/1.0/azure-storage-5.0.0.jar  
	More recent versions are available at: https://github.com/Azure/azure-storage-java
* magi_2.11-1.0.jar  
	Available at: https://github.com/NTAP/magi/releases/download/1.0/magi_2.11-1.0.jar

## How to compile from source code? ##
1. Compile magi
~~~~
$ git clone git@github.com:NTAP/magi.git
$ cd magi
$ sbt package
$ ls target/scala-2.11/magi_2.11-0.2.jar
~~~~

The jar file is ready. 

## How to run it? ##
1. You need a spark cluster up and running, either at Microsoft Azure, AWS EC2 or your own data center. 
2. Install the two Java SDK jars in your spark cluster (assume spark is installed at /usr/spark.).
	~~~~
	$ scp aws-java-sdk-1.11.84.jar each-spark-node:/usr/spark/jars
	$ scp azure-storage-5.0.0.jar each-spark-node:/usr/spark/jars 
	~~~~
3. Copy the magi jar file to the spark master node.
	~~~~
	$ scp target/scala-2.11/magi_2.11-1.0.jar spark-master-node:~/
	~~~~


### Bucket synchronization ### 
* Function: replicate/synchronize objects between two buckets sitting in S3, Azure or NetApp StorageGrid.
* Options
  * --originBucket bucket, specify the origin bucket 
  * --destBucket bucket, specify the destination bucket
  * --delete, delete objects in the origin bucket after the synchronization
  * --dryrun, skip the copy process
  * --prefix [numeric|letters|hex|alphanumerics|all], specify which set of prefixes to use
  * --verbose, log all missing objects in output

1. Configure cloud endpoints and credentials in spark configuration file.
	~~~~
	vim /usr/spark/conf/spark-defaults.conf
	// Example 1. Use Azure as the origin
	+ spark.sb.origin                    azure
	+ spark.sb.origin.account            storageaccount
	+ spark.sb.origin.secretkey          secretkey

	// Example 2. Use S3 as the destination
	+ spark.sb.destination               s3
	+ spark.sb.dest.account              s3account
	+ spark.sb.dest.secretkey            s3secretkey
	+ spark.sb.dest.region               us-east-1

	// Example 3. Use NetApp StorageGrid as the origin
	+ spark.sb.origin                    storagegrid
	+ spark.sb.origin.account            storagegridaccount
	+ spark.sb.origin.secretkey          storagegridsecretkey
	+ spark.sb.origin.endpointURL        https://storagegridhostname:port

	~~~~

	NOTE: For AWS S3, we need to specify which region the bucket resides, by setting a value for the property "spark.sb.[origin/dest].region". For NetApp StorageGrid, we need to specify the endpoint URL, by setting a value for the property "spark.sb.[origin/dest].endpointURL". 
	
2. Submit the job
	~~~~
	$ /usr/spark/bin/spark-submit --master spark://spark-master-node-ip:7077 \
	--class SyncBucket  ~/magi_2.11-0.2.jar --originBucket originbucket \
	--destBucket destinationBucket > bucket-sync.log
	~~~~

### Utility Functions ###
We also implemented three other functions that can help for testing purposes. 
They share the same set of configuration parameters. 
#### Configuration ####
1. Configuration for Azure
	~~~~
	vim /usr/spark/conf/spark-defaults.conf
	+ spark.cloud                    azure
	+ spark.cloud.account            storageaccount
	+ spark.cloud.secretkey          secretkey
	~~~~
2. Configuration for AWS S3
	~~~~
	vim /usr/spark/conf/spark-defaults.conf
	+ spark.cloud                    s3
	+ spark.cloud.account            s3account
	+ spark.cloud.secretkey          s3secretkey
	+ spark.cloud.region             us-east-1
	~~~~
3. Configuration for NetApp StorageGrid
	~~~~
	vim /usr/spark/conf/spark-defaults.conf
	+ spark.cloud                    storagegrid
	+ spark.cloud.account            storagegridaccount
	+ spark.cloud.secretkey          storagegridsecretkey
	+ spark.cloud.endpointURL        https://storagegridhostname:port
	~~~~

#### Empty a bucket ####
* Function: delete all objects in a bucket
* Options
  * --bucket bucket, specify the bucket to delete objects
  * --dryrun, run the program without actually issuing delete operations
  * --prefix [numeric|letters|hex|alphanumerics|all], specify which set of prefixes to use

* Submit the job
	~~~~
	$ /usr/spark/bin/spark-submit --master spark://spark-master-node-ip:7077 \
	--class EmptyBucket  ~/magi_2.11-0.2.jar --bucket bucket > bucket-empty.log
	~~~~

#### List a bucket ####
* Function: list all objects in a bucket
* Options
  * --bucket bucket, specify the bucket to list
  * --summary, only print the total number of ojects
  * --prefix [numeric|letters|hex|alphanumerics|all], specify which set of prefixes to use

* Submit the job
	~~~~
	$ /usr/spark/bin/spark-submit --master spark://spark-master-node-ip:7077 \
	--class ListBucket  ~/magi_2.11-0.2.jar --bucket bucket > bucket-list.log
	~~~~

#### Fill a bucket ####
* Function: fill a bucket with objects. The last 4 bytes of each object is the 32-bit CRC checksum calculated based on the previous bytes of the object.   
* Options
  * --bucket bucket, specify the bucket to populate
  * --size size, specify object size in bytes
  * --count count, specify the number of objects to populate
  * --prefix [numeric|letters|hex|alphanumerics|all], specify which set of prefixes to use

* Submit the job
	~~~~
	$ /usr/spark/bin/spark-submit --master spark://spark-master-node-ip:7077 \
	--class FillBucket  ~/magi_2.11-0.2.jar --bucket bucket \
	--size size --count 1000 > bucket-fill.log
	~~~~

## Questions? ##
While this is not a product from NetApp, we do welcome feedbacks. Please contact ng-cloud-management-kits@netapp.com for feedbacks and questions. 
