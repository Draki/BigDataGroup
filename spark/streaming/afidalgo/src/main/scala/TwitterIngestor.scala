import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

/**
 * Class to ingest data twiter
 */
class TwitterIngestor {

  private val sparkConf = new SparkConf().setAppName("TwiterIngestor").setMaster("local[*]")
  private val context = new StreamingContext(sparkConf,Seconds(1))
  private val twitterAuthorizer = new TwitterAuthorizationBuilder(ConfigFactory.load()).build()

  def ingestTwiterTags(): Unit ={
    val filters = Array("Real Madrid")
    val stream = TwitterUtils.createStream(context,Some(twitterAuthorizer.getAuthorization()),filters)
    val hashTags = stream
                .map(status => (status.getCreatedAt,status.getText))
    hashTags.foreachRDD(rdd => {
      val topList = rdd.collect()
      topList.foreach{text => println("%s : ".concat(text._2))}
    })

    context.start()
    context.awaitTermination()
  }
}



