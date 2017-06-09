package managekitsconf;
import org.apache.spark.SparkConf
import storageclient._

@SerialVersionUID(1010L)
class Conf(var cloudEP: CloudEndPoint, var bucket: String, var account: String, var secretKey: String) 
			extends Serializable
{
	override def toString(): String = "cloud: " + cloudEP + "\n\tbucket: " + bucket + 
				"\n\tAccount: " + account + "\n\tSecretKey: " + secretKey;
}

object GetConf {
	def get (sparkConf: SparkConf, bucket: String): Conf = {
		// get configuration for the cloud
		val cloud = sparkConf.get("spark.cloud")
		val region = sparkConf.get("spark.cloud.region", "")
		val endpointURL = sparkConf.get("spark.cloud.endpointURL", "")
		val account = sparkConf.get("spark.cloud.account")
		val secretKey = sparkConf.get("spark.cloud.secretkey")

		val cloudEP = new CloudEndPoint(cloud, region, endpointURL)
		cloudEP.validate()

		new Conf(cloudEP, bucket, account, secretKey)
	}
}

object Constants {
	val RePartitionNum = 500
}