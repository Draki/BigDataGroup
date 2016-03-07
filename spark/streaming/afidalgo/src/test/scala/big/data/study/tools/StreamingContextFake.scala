package big.data.study.tools

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable

import twitter4j.Status


class StreamingContextFake [T](manualClockClass:String) {

  private val conf = new SparkConf()
                    .setMaster("local[*]")
                    .setAppName("test AppName")
                    .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
  private val sc = new SparkContext(conf)
  private val lines = mutable.Queue[RDD[Status]]()

  val ssc =  new StreamingContext(sc, new Duration(1))


  def createDStrem : DStream[Status] = {
    ssc.queueStream(lines)
  }

  def addDataInRDD(seq:Seq[Status]): Unit ={
    lines += sc.makeRDD(seq)
  }

  def start : Unit =  {
    ssc.start()
  }

  def stop: Unit = {
    ssc.stop()
    sc.stop()
  }
}
