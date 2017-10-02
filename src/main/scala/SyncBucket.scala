import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


import scala.collection.JavaConversions._
import java.io._	// for inputstream
import java.nio.file._

import storageclient._
import prefix._

/** Configuration parameters for synchronizing between two buckets. 
 *
 *  It includes cloud provider, bucket, account, and secrectkey for both origin cloud and destination cloud. 
 */
@SerialVersionUID(1010L)
class CMConf(var origin: CloudEndPoint, var originBucket: String, var originAccount: String, var originSecretKey: String, 
			var destination: CloudEndPoint, var destBucket: String, var destAccount: String, var destSecretKey: String) 
			extends Serializable
{	
	override def toString(): String = "origin: " + origin + "\n\tBucket: " + originBucket + 
				"\n\tAccount: " + originAccount + "\n\tSecretKey: " + originSecretKey +
				"\ndestination: " + destination + "\n\tBucket: " + destBucket + 
				"\n\tAccount: " + destAccount + "\n\tSecretKey: " + destSecretKey;
}

/** main object implementing bucket synchronization 
 *
 *  Input parameters:
 *     --originBucket bucket, origin bucket 
 *     --destBucket bucket, destination bucket
 *     --delete, delete objects from the origin bucket after synchronization
 *     --prefix [numeric|letters|hex|alphanumerics|all], specify which prefix set to use
 *     --dryrun, skip the step of copying objects 
 *     --partitions partitions, the number of partitions (tasks)
 *     --verbose, turn on verbose output
 *
 *	This scala program implements the function to synchronize between two buckets.
 *  It first gets a list of all objects in the origin bucket, then gets the list
 *  of objects in the destination bucket. After that, it subtracts objects in the
 *  destination bucket from these in the origin bucket, to get the list of objects
 *  that need to be copied over. It then copies each object from the origin bucket
 *  to the destination bucket. Finally, it deletes all objects from the origin 
 *  bucket if that is specified. 
 */
object SyncBucket {

	/** List all objects in a cloud bucket with the specified prefix. */
	def list(whichEnd: String, cmConf: CMConf, prefix: String) = {
		whichEnd.toLowerCase match {
			case "origin" => {
				val credential = new Credential(cmConf.originAccount, cmConf.originSecretKey)
				val client = GetStorageClient.get(cmConf.origin, credential)
				client.list(cmConf.originBucket, prefix)
			} 
			case "destination" => {
				val credential = new Credential(cmConf.destAccount, cmConf.destSecretKey)
				val client = GetStorageClient.get(cmConf.destination, credential)
				client.list(cmConf.destBucket, prefix)
			}
			case _ => throw new IllegalArgumentException("Invalid value in list(): " + whichEnd)
		}
	}

	/** Copy an object from origin cloud to destination cloud. 
	 *  
	 *  It downloads from origin cloud and uploads to destination cloud.	
	 */
	def copy(cmConf: CMConf, obj: String) = {
		val srcCredentail = new Credential(cmConf.originAccount, cmConf.originSecretKey)
		val origin = GetStorageClient.get(cmConf.origin, srcCredentail)
		val instream: InputStream = origin.get(cmConf.originBucket, obj)

		val destCredential = new Credential(cmConf.destAccount, cmConf.destSecretKey)
		val destination = GetStorageClient.get(cmConf.destination, destCredential)
		destination.put(cmConf.destBucket, obj, instream)
	}
	
	/** Delete an object in the origin bucket. */
	def deleteFromOrigin(cmConf: CMConf, obj: String) = {
		val srcCredentail = new Credential(cmConf.originAccount, cmConf.originSecretKey)
		val origin = GetStorageClient.get(cmConf.origin, srcCredentail)
		origin.del(cmConf.originBucket, obj)
	}

	def main(args: Array[String]) {
		var starttime: Long = System.currentTimeMillis
		var runtime : Double = 0.0

		var originBucket: Option[String] = None
		var destBucket: Option[String] = None
		var prefix: Option[String] = Some("all")
		var partitions: Option[Int] = None
		var delete = false
		var verbose = 0
		var dryrun = false

		// Check for --originBucket and --destBucket parameters
		args.sliding(2, 1).toList.collect {
			case Array("--originBucket", origin: String) => originBucket = Some(origin)
			case Array("--destBucket", destination: String) => destBucket = Some(destination)
			case Array("--prefix", p: String) => prefix = Some(p)
			case Array("--partitions", p: String) => partitions = Some(p.toInt)
		}
		// Check for --delete and --verbose parameters
		args.sliding(1, 1).toList.collect {
			case Array("--delete") => delete = true
			case Array("--dryrun") => dryrun = true
			case Array("--verbose") => verbose += 1
		}

		if (originBucket.isEmpty || destBucket.isEmpty) 
			 throw new IllegalArgumentException("Usage: SyncBucket --originBucket bucket --destBucket bucket [--delete] [--prefix prefixtype]")

		println("SyncBucket: originBucket=" + originBucket.get + ", destBucket=" + destBucket.get + ", delete=" + delete.toString +
			", verbose=" + verbose)
		val conf = new SparkConf().setAppName("SyncBucket")
		val sc = new SparkContext(conf)

		// get configuration for origin cloud
		val origin = conf.get("spark.sb.origin")
		val originRegion = conf.get("spark.sb.origin.region", "")
		val originEndPointURL = conf.get("spark.sb.origin.endpointURL", "")
		val originAccount = conf.get("spark.sb.origin.account")
		val originSecretKey = conf.get("spark.sb.origin.secretkey")

		// get configuration for destination cloud
		val destination = conf.get("spark.sb.destination")
		val destRegion = conf.get("spark.sb.dest.region", "")
		val destEndPointURL = conf.get("spark.sb.dest.endpointURL", "")
		val destAccount = conf.get("spark.sb.dest.account")
		val destSecretKey = conf.get("spark.sb.dest.secretkey")

		val originEndPoint = new CloudEndPoint(origin, originRegion, originEndPointURL)
		val destEndPoint = new CloudEndPoint(destination, destRegion, destEndPointURL)
		originEndPoint.validate()
		destEndPoint.validate()
		
		val cmConf = new CMConf(originEndPoint, originBucket.get, originAccount, originSecretKey, 
								destEndPoint, destBucket.get, destAccount, destSecretKey)
		println(cmConf)
	
		// start to do real stuff
		val prefixes = PrefixGenerator.generate(prefix.get, 2)
		println("Total number of prefixes: " + prefixes.length)
		
		if (partitions.isEmpty)
			partitions = Some(prefixes.length)

		val prefixRDD = sc.parallelize(prefixes, partitions.get)
		
		// Get objects in origin cloud 
		val originObjsRDD = prefixRDD.flatMap(p => list("origin", cmConf, p))
		println(f"Objects in origin: " + originObjsRDD.count)
		
		// Get objects in destination cloud
		val destObjsRDD = prefixRDD.flatMap(p => list("destination", cmConf, p))
		println(f"Objects in destination:  " + destObjsRDD.count)

		// Get objects that are missing from destination cloud
		val missObjsRDD = originObjsRDD.subtract(destObjsRDD)
		println(f"Objects need to be migrated: " + missObjsRDD.count)
		if (verbose > 0 && missObjsRDD.count > 0) {
			println("Missing Objects: ")
			missObjsRDD.collect.map(o => println("\t"+o))
		}
		
		// Copy missing objects to destination cloud
		var totalObjects = 0L
		if (!dryrun) {
			val results = missObjsRDD.map(o => copy(cmConf, o))
			totalObjects = results.count
			println("Objects processed = " + totalObjects + ", partitions = " + missObjsRDD.partitions.size)
		}

		// Delete objects in origin cloud if needed
		if (delete) {
			val deleted = originObjsRDD.map(o => deleteFromOrigin(cmConf, o))
			println("Objects deleted from origin: " + deleted.count)
		}

		runtime = (System.currentTimeMillis - starttime)/1000.0
		println ("Exit from SyncBucket: runtime = %.2f minutes, throughput = %d objects/sec"
			.format(runtime/60.0, (totalObjects/runtime).toInt))
		sc.stop()
	}
} 
