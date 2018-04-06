import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._
import java.nio._
import java.io._
import java.util.UUID.randomUUID	// for generating random object ids
import java.util.zip.CRC32 			// for calculating CRC32
import scala.collection.mutable.ListBuffer
import storageclient._
import managekitsconf._
import prefix._


/** This scala program implements the function to fill a bucket with a certain number
 *  of objects at a particular size.
 *
 *  Input parameters:
 *    --bucket bucket, the bucket to fill
 *    --size size, object size
 *    --count count, the number of objects to create
 *    --prefix [numeric|letters|hex|alphanumerics|all], specify which prefix set to use
 *    --partitions partitions, the number of partitions (tasks) 
 *
 *  It first generates the prefix list, then determines how many objects to create for each prefix (rounded up) 
 *  and finally calls fill() to create objects for each prefix. 
 */
object FillBucket {

	/* Fill the bucket with objects with a specific prefix. 
	 *
	 * It first checks how many objects have already been created in the bucket
	 * and then decides how many additional objects to create. This is mainly
	 * to handle the case where we see "Internal Service Error" from the server-side.
	 * Such errors will lead to task failures and restarts.
	 */
	def fill(conf: Conf, prefix: String, size: Int, count: Int) = {
		var objects = new ListBuffer[String]()
		val credential = new Credential(conf.account, conf.secretKey)
		val storageClient = GetStorageClient.get(conf.cloudEP, credential)
		val objectsinCloud = storageClient.list(conf.bucket, prefix).length
		val objectsToCreate = count - objectsinCloud
		var metaData = new java.util.HashMap[String, String]()
		metaData.put("x-amz-meta-creator", "netapp-magi")

		println("[INFO]: fill() bucket=" + conf.bucket + ", objectsinCloud=" + 
			objectsinCloud + ", objectsToCreate=" + objectsToCreate)
		var buffer = ByteBuffer.allocate(size)
		for (i <- 0 until size) buffer.put(i, i.toByte)

		// calculate CRC32
		var crc32 = new CRC32()
		crc32.update(buffer.array, 0, size-4)
		val crc: Int = crc32.getValue.toInt
		buffer.putInt(size-4, crc)

		val value: ByteArrayInputStream = new ByteArrayInputStream(buffer.array)

		for (i <- 0 until objectsToCreate) {
			val uuid = randomUUID.toString
			val obj = prefix + uuid
			storageClient.put(conf.bucket, obj, value, metaData)
			objects += obj
			value.reset
		}
		objects.toList
	}

	def main(args: Array[String]) {
		var starttime: Long = System.currentTimeMillis
		var runtime : Double = 0.0

		var bucket: Option[String] = None
		var size: Option[Int] = None
		var count: Option[Int] = None
		var prefix: Option[String] = Some("all")
		var partitions: Option[Int] = None

		// Check for --originBucket and --destBucket parameters
		args.sliding(2, 1).toList.collect {
			case Array("--bucket", origin: String) => bucket = Some(origin)
			case Array("--size", s: String) => size = Some(s.toInt)
			case Array("--count", c: String) => count = Some(c.toInt)
			case Array("--partitions", p: String) => partitions = Some(p.toInt)
			case Array("--prefix", p: String) => prefix = Some(p)
		}

		if (bucket.isEmpty || size.isEmpty || count.isEmpty) 
			throw new IllegalArgumentException("Usage: FillBucket --bucket bucket --size size --count count [--prefix prefixtype]")
		//if (count.get % 100 != 0)
		//	throw new IllegalArgumentException("object count is not multiples of 100")
		if (size.get <= 0)
			throw new IllegalArgumentException("object size should be a positive number")	

		println("FillBucket: bucket = " + bucket.get + ", size = " + size.get + ", count = " + count.get)
		val sparkConf = new SparkConf().setAppName("Fill Bucket")
		val sc = new SparkContext(sparkConf)

		// get configuration for origin cloud
		val emConf = GetConf.get(sparkConf, bucket.get)
		println(emConf)
	
		// start to do real stuff
		val prefixes = PrefixGenerator.generate(prefix.get, 2)
		// println("Total number of prefixes: " + prefixes.length)

		// if partitions is not set, set it to the number of prefixes.
		if (partitions.isEmpty)
			partitions = Some(prefixes.length)

		val prefixRDD = sc.parallelize(prefixes, partitions.get)
		
		val countPerPrefix = (count.get + prefixes.length-1)/prefixes.length
		println("Number of prefixes = " + prefixes.length + ", countPerPrefix = " + countPerPrefix 
			+ ", partitions = " + prefixRDD.partitions.size)

		// Get objects in the bucket 
		val ObjsRDD = prefixRDD.flatMap(p => fill(emConf, p, size.get, countPerPrefix))
		val totalObjects = ObjsRDD.count
		println("Objects created = " + totalObjects + ", partitions = " + ObjsRDD.partitions.size)

		runtime = (System.currentTimeMillis - starttime)/1000.0
		println("Exit from ListBucket: runtime = %.2f minutes, throughput = %d objects/sec"
			.format(runtime/60, (totalObjects/runtime).toInt))
		sc.stop()
	}
} 
