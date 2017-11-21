# Use magi to copy from an S3 bucket to another S3 bucket. #


## Software ##
* Spark-2.1.0-bin-hadoop2.7
* aws-java-sdk-1.11.84.jar  
	Available at: https://aws.amazon.com/sdk-for-java/
* azure-storage-5.0.0.jar  
	Available at: https://github.com/Azure/azure-storage-java


## How to compile it? ##
~~~~
$ git clone git@github.com:NTAP/magi.git
$ cd magi
$ sbt package
$ ls target/scala-2.11/magi_2.11-0.2.jar
~~~~

The jar file is ready. 

## How to run it? ##
1. You need a spark cluster up and running, either at Microsoft Azure, AWS EC2 or your own data center. 
2. Get the AWS Java SDKs . 
    * AWS Java SDK. We can download using the following command.  
	~~~~
	$ wget https://sdk-for-java.amazonwebservices.com/latest/aws-java-sdk.zip
	$ unzip aws-java-sdk-1.11.84.zip
	$ ls aws-java-sdk-1.11.84/lib/aws-java-sdk-1.11.84.jar
	~~~~
3. Install the above jars in your spark cluster (assume spark is installed at /usr/spark.).
	~~~~
	$ scp aws-java-sdk-1.11.84/lib/aws-java-sdk-1.11.84.jar each-spark-node:/usr/spark/jars
	~~~~
4. Copy the magi jar file to the spark master node.
	~~~~
	$ scp target/scala-2.11/magi_2.11-1.0.jar spark-master-node:~/
	~~~~

### Bucket synchronization ### 
* Function: replicate/synchronize objects between two buckets sitting in S3.
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
	// Set S3 as the origin
	+ spark.sb.origin                    s3
	+ spark.sb.origin.account            s3account
	+ spark.sb.origin.secretkey          s3secretkey
	+ spark.sb.origin.region             us-west-1

	// Set S3 as the destination
	+ spark.sb.destination               s3
	+ spark.sb.dest.account              s3account
	+ spark.sb.dest.secretkey            s3secretkey
	+ spark.sb.dest.region               us-east-1

	~~~~

	
2. Submit the job from the spark master node
	~~~~
	$ /usr/spark/bin/spark-submit --master spark://spark-master-node-ip:7077 \
	--class SyncBucket  ~/magi_2.11-1.0.jar --originBucket originbucket \
	--destBucket destinationBucket > bucket-sync.log
	~~~~

### Utility Functions ###
We also implemented three other functions that can help for testing purposes. 
They share the same set of configuration parameters. 
#### Configuration ####
1. Configuration for AWS S3
	~~~~
	vim /usr/spark/conf/spark-defaults.conf
	+ spark.cloud                    s3
	+ spark.cloud.account            s3account
	+ spark.cloud.secretkey          s3secretkey
	+ spark.cloud.region             us-east-1
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
