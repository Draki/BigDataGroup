package big.data.study.fakes

import big.data.study.tools.StreamingContextFake
import org.apache.spark.streaming.StreamingContextWrapper

class ClockWrapper[T](sscFake: StreamingContextFake[T]) {

  private val manualClock = new StreamingContextWrapper(sscFake.ssc).manualClock

  def advance(timeToAdd: Long) = manualClock.advance(timeToAdd)

}

