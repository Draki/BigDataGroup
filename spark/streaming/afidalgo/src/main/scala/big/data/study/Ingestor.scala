package big.data.study


import big.data.study.persist.PersistFactory
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status


class Ingestor (persistFactory:PersistFactory) {


  def ingest(dStream: DStream[Status]): Unit = {
    dStream.foreachRDD(status =>{
                         status.collect().foreach({
                              status =>{
                                   val tupleInsert = (status.getText,status.getCreatedAt)
                                   val persists =   persistFactory.getPersist(tupleInsert._1)
                                   persists.foreach(persist=>persist.insert(tupleInsert))
                               }
                          }

                   )
           })
  }

}
