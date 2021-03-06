package big.data.study

import big.data.study.persist.PersistFactory
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

class TwitterIngestor {

  private val sparkConf = new SparkConf()
                                .setAppName("TwiterIngestor")
                                .setMaster("local[*]")
  private val context = new StreamingContext(sparkConf,Seconds(1))
  private val twitterAuthorized = new TwitterAuthorizationBuilder(ConfigFactory.load()).build()

  def ingestTwiterTags(): Unit ={

    val filters = Array("Real Madrid","Barcelona")
    new Ingestor(PersistFactory())
                .ingest(
                     TwitterUtils.createStream(context,Some(twitterAuthorized.getAuthorization),filters)
                  )
    context.start()
    context.awaitTermination()
  }
}



