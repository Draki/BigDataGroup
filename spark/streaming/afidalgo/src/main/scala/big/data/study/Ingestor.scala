package big.data.study


import big.data.study.persist.Persist
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

//TODO : CREATE TEST THAT SELECTED TYPE OF PERSIST
class Ingestor (persist:Persist) {


  def ingest(dStream: DStream[Status]): Unit = {
    dStream.foreachRDD(status =>{
                         status.collect().foreach({
                              status => persist.insert((status.getText,status.getCreatedAt))
                          }

                   )
           })
  }

}
