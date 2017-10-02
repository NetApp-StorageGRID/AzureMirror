import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._
import java.nio.file._

import storageclient._
import managekitsconf._
import prefix._


/** This scala program implements the function to list objects in a bucket.
 *
 *  Input parameters:
 *     --bucket bucket, the bucket to list
 *     --summary, show summary information (total number of objects)
 *     --prefix [numeric|letters|hex|alphanumerics|nonnumeric|all], specify which prefix set to use
 *     --partitions partitions, the number of partitions (tasks)
 */
object ListBucket {
	/* list objects for a particular prefix. */
	def list(conf: Conf, prefix: String) = {
		val credential = new Credential(conf.account, conf.secretKey)
		val storageClient = GetStorageClient.get(conf.cloudEP, credential)
		storageClient.list(conf.bucket, prefix)
	}

	def main(args: Array[String]) {
		var starttime: Long = System.currentTimeMillis
		var runtime : Double = 0.0

		var bucket: Option[String] = None
		var prefix: Option[String] = Some("all")
		var partitions: Option[Int] = None
		var summary = false

		// Check for --bucket parameter
		args.sliding(2, 1).toList.collect {
			case Array("--bucket", origin: String) => bucket = Some(origin)
			case Array("--prefix", p: String) => prefix = Some(p)
			case Array("--partitions", p: String) => partitions = Some(p.toInt)
		}

		// Check for --summary parameter
		args.sliding(1, 1).toList.collect {
			case Array("--summary") => summary = true
		}

		if (bucket.isEmpty) 
			 throw new IllegalArgumentException("Usage: ListBucket --bucket bucket [--summary] [--prefix prefixtype]")

		println("ListBucket: bucket=" + bucket.get + ", summary=" + summary.toString)
		val sparkConf = new SparkConf().setAppName("List Bucket")
		val sc = new SparkContext(sparkConf)

		val emConf = GetConf.get(sparkConf, bucket.get)

		println(emConf)
	
		// start to do real stuff
		val prefixes = PrefixGenerator.generate(prefix.get, 2)
		println("Total number of prefixes: " + prefixes.length)
		
		if (partitions.isEmpty)
			partitions = Some(prefixes.length)

		val prefixRDD = sc.parallelize(prefixes, partitions.get)
		
		// Get objects in the bucket 
		val ObjsRDD = prefixRDD.flatMap(p => list(emConf, p))
		val totalObjects = ObjsRDD.count
		println("Objects in bucket = " + totalObjects + ", partitions = " + ObjsRDD.partitions.size)
		
		// print out the list of objects
		if (summary == false) {
			ObjsRDD.collect.map(println)
		}

		runtime = (System.currentTimeMillis - starttime)/1000.0
		println("Exit from ListBucket: runtime = %.2f minutes, throughput = %d objects/sec"
			.format(runtime/60, (totalObjects/runtime).toInt))
		sc.stop()
	}
} 
