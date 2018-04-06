package storageclient;

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.S3ClientOptions
import com.amazonaws.client.builder.AwsClientBuilder._
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.model._
import com.amazonaws.regions.Regions

import com.microsoft.azure.storage._
import com.microsoft.azure.storage.blob._
import java.io._	// for inputstream
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/** CloudEndPoint. */
@SerialVersionUID(1010L)
class CloudEndPoint(var cloud: String, var region: String = "", var endpointURL: String = "") extends Serializable
{
	override def toString(): String = {
		var ret = "cloud=" + cloud
		if (!region.isEmpty)
			ret = ret + ", region=" + region
		if (!endpointURL.isEmpty)
			ret = ret + ", endpoint=" + endpointURL
		ret
	}
	def validate() {
		if (cloud.toLowerCase == "s3" && region.isEmpty)
			throw new IllegalArgumentException("region for S3 is not set!")
		if (cloud.toLowerCase == "storagegrid" && endpointURL.isEmpty)
			throw new IllegalArgumentException("endpoint url for storageGrid is not set!")	
	}
}

/** Credential information (account and key). */
class Credential(var account: String, var secretKey: String) {
	override def toString(): String = "account = " + account + ", secretKey = " + secretKey;
}



/** This is the abstract class for storage client 
 *
 *  Every storage client needs to implement the following functions. 
 *  In this implementation, we implement a storage client for S3, Azure and StorageGrid. 
 *  S3Client and AzureClient extends from this abstract class directly while
 *  StorageGridClient extends from S3Client.
 */
abstract class StorageClient {
	def list(bucket: String, prefix: String) : List[String]
	def get(bucket: String, objID: String) : (InputStream, java.util.Map[String,String])
	def put(bucket: String, objID: String, objValue: InputStream)
	def del(bucket: String, objID: String)
}

/** Storage client for S3
 *
 *  Hard-code region to us-east-1
 *
 *  It implements functions defined in StorageClient class. 
 */
class S3Client(var credential: Credential, var region: String) extends StorageClient {
	val awsCreds = new BasicAWSCredentials(credential.account, credential.secretKey)
	val s3Client = AmazonS3ClientBuilder.standard()
					.withRegion(region)
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
					.build()

	def list(bucket: String, prefix: String) = {
		println("s3 list: bucket=" + bucket + ", prefix=" + prefix)
		var objects = new ListBuffer[String]()

		// fetch all objects in the bucket and store them into a list 

		var objList = s3Client.listObjects(
						new ListObjectsRequest()
							.withBucketName(bucket)
							.withPrefix(prefix)
							)

		while (objList.getObjectSummaries.size > 0) {
			val objs = objList.getObjectSummaries()
			
			for (obj <- objs) objects += obj.getKey()
			
			objList = s3Client.listNextBatchOfObjects(objList)
		}

		// for (obj <- objects.take(20)) println(obj)
		println(" object count: " + objects.length)
		objects.toList
	}

	def get(bucket: String, objID: String) : (InputStream, java.util.Map[String,String]) = {
		val obj: S3Object = s3Client.getObject(new GetObjectRequest(bucket, objID))
		val objectContent: InputStream = obj.getObjectContent
		val metaData = obj.getObjectMetadata().getUserMetadata()
		(objectContent, metaData)
	}

	def put(bucket: String, objID: String, objectContent: InputStream) = {
		s3Client.putObject(new PutObjectRequest(bucket, objID, objectContent, null))
	}

	def del(bucket: String, objID: String) = {
		s3Client.deleteObject(new DeleteObjectRequest(bucket, objID))
	}
}

/** Storage client for Azure */
class AzureClient(var credential: Credential) extends StorageClient {
	val storageConnectionString = 
			"DefaultEndpointsProtocol=http;" + 
			"AccountName=" + credential.account + ";" + 				
			"AccountKey=" + credential.secretKey
		
	//println(storageConnectionString)
	val storageAccount = CloudStorageAccount.parse(storageConnectionString)
	val blobClient = storageAccount.createCloudBlobClient()

	def list(bucket: String, prefix: String) = {
		println("azure list: bucket=" + bucket + ", prefix=" + prefix)
		val containerClient = blobClient.getContainerReference(bucket)
		
		var blobs = containerClient.listBlobs(prefix)
		var blobIDs = blobs.map(b => b.getUri.toString.split('/').last).toList

		println("  object count: " + blobIDs.length)
		blobIDs
	}

	def get(bucket: String, objID: String) : (InputStream, java.util.Map[String,String]) = {
		val containerClient = blobClient.getContainerReference(bucket)
		val blob = containerClient.getBlockBlobReference(objID)
		val outstream = new ByteArrayOutputStream()
		blob.download(outstream)
		blob.downloadAttributes()
		val objectContent = new ByteArrayInputStream(outstream.toByteArray)
		val metaData = blob.getMetadata()
		(objectContent, metaData)
	}

	def put(bucket: String, objID: String, objectContent: InputStream) = {
		val containerClient = blobClient.getContainerReference(bucket)
		val blob = containerClient.getBlockBlobReference(objID)

		blob.upload(objectContent, -1)
	}

	def del(bucket: String, objID: String) = {
		val containerClient = blobClient.getContainerReference(bucket)
		val blob = containerClient.getBlockBlobReference(objID)
		blob.deleteIfExists();
	}
}

package object GetStorageClient {
	def getS3Client(cloudEP: CloudEndPoint, credential: Credential) = {
		if (cloudEP.region.isEmpty)
			throw new IllegalArgumentException("Region for S3 cloud is not set!")	 
		
		new S3Client(credential, cloudEP.region)
	}

	def getStorageGridClient(cloudEP: CloudEndPoint, credential: Credential) = {

		if (cloudEP.endpointURL.isEmpty)
			throw new IllegalArgumentException("EndpointURL for StorageGrid is not set!")

		new StorageGridClient(credential, cloudEP.endpointURL)	
	}

	/** Get a storage client based on which cloud provider is requiring for. */
	def get(cloudEP: CloudEndPoint, credential: Credential) : StorageClient = {
		cloudEP.cloud.toLowerCase match {
			case "s3" => getS3Client(cloudEP, credential)
			case "azure" => new AzureClient(credential)
			case "storagegrid" => getStorageGridClient(cloudEP, credential)
			case _ => throw new IllegalArgumentException("cloud provider is invalid: " + cloudEP.cloud)
		}
	}

}

class StorageGridClient(credential: Credential, endpointURL: String) extends S3Client (credential, "us-east-1") {
	override val s3Client = AmazonS3ClientBuilder.standard()
					.withEndpointConfiguration(new EndpointConfiguration(endpointURL, ""))
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
					.enablePathStyleAccess()
					.build()
}