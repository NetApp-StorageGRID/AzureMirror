import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._
import java.nio.file._

import storageclient._
import managekitsconf._
import prefix._

/** This scala program implements the function to delete all objects in a bucket.
 *
 *  Input parameters:
 *    --bucket bucket, the bucket to remove objects
 *    --prefix [numeric|letters|hex|alphanumerics|all], specify which prefix set to use
 *    --partitions partitions, the number of partitions (tasks)
 *    --dryrun, skip the delete process
 *
 *  We first get a list of objects and then delete each object.
 */
object EmptyBucket {

	/* List objects in the bucket with the specified prefix. */
	def list(conf: Conf, prefix: String) = {
		val credential = new Credential(conf.account, conf.secretKey)
		val storageClient = GetStorageClient.get(conf.cloudEP, credential)
		storageClient.list(conf.bucket, prefix)
	}
	
	/** Delete an object in the bucket */
	def delete(conf: Conf, obj: String) = {
		val credentail = new Credential(conf.account, conf.secretKey)
		val storageClient = GetStorageClient.get(conf.cloudEP, credentail)
		storageClient.del(conf.bucket, obj)
	}

	def main(args: Array[String]) {
		var starttime: Long = System.currentTimeMillis
		var runtime : Double = 0.0

		var bucket: Option[String] = None
		var prefix: Option[String] = Some("all")
		var partitions: Option[Int] = None
		var dryrun = false

		// Check for --bucket parameter
		args.sliding(2, 1).toList.collect {
			case Array("--bucket", origin: String) => bucket = Some(origin)
			case Array("--prefix", p: String) => prefix = Some(p)
			case Array("--partitions", p: String) => partitions = Some(p.toInt)
		}
		// Check for --dryrun parameter
		args.sliding(1, 1).toList.collect {
			case Array("--dryrun") => dryrun = true
		}

		if (bucket.isEmpty) 
			 throw new IllegalArgumentException("Usage: EmptyBucket --bucket bucket [--dryrun] [--prefix prefixtype]")

		println("EmptyBucket: bucket=" + bucket.get + ", dryrun=" + dryrun.toString)
		val sparkConf = new SparkConf().setAppName("Empty Bucket")
		val sc = new SparkContext(sparkConf)

		// get configuration for the cloud
		val emConf = GetConf.get(sparkConf, bucket.get)
		println(emConf)
	
		// start to do real stuff
		val prefixes = PrefixGenerator.generate(prefix.get, 2)
		// println("Total number of prefixes: " + prefixes.length)

		// if partitions is not set, use the number of prefixes	
		if (partitions.isEmpty)
			partitions = Some(prefixes.length)

		val prefixRDD = sc.parallelize(prefixes, partitions.get)
		
		// Get objects in the bucket 
		val ObjsRDD = prefixRDD.flatMap(p => list(emConf, p))

		//val ObjsRDD = OriginObjsRDD.repartition(Constants.RePartitionNum)
		//println("Objects in bucket: " + ObjsRDD.count + ", partitions=" + ObjsRDD.getNumPartitions)
		
		var totalObjects = 0L
		// delete the objects if not in dryrun mode
		if (dryrun == false) {
			val deleted = ObjsRDD.map(o => delete(emConf, o))
			totalObjects = deleted.count 
			println("Objects deleted from bucket = " + totalObjects + ", partitions = " + ObjsRDD.partitions.size)
		}

		runtime = (System.currentTimeMillis - starttime)/1000.0
		println("Exit from EmptyBucket: runtime = %.2f minutes, throughput = %d objects/sec"
			.format(runtime/60, (totalObjects/runtime).toInt))
		sc.stop()
	}
} 
