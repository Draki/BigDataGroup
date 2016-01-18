package sparksql

import org.apache.spark.{SparkContext, SparkConf}


class Context (hostCassandra:String,hostConnector:String){


  val conf = new SparkConf(true)
                   .set("spark.cassandra.connection.host","127.0.0.1")
  val sc = new SparkContext(hostCassandra, "test", conf)


  def context:SparkContext={
        return sc
  }


  def stop{
     sc.stop()
  }

}
