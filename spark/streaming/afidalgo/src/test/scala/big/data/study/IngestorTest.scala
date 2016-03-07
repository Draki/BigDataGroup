package big.data.study


import java.util.Date

import big.data.study.doubles.StatusDouble
import big.data.study.matchers.{TupleMatcher, DateMatcher, StringMatcher}
import big.data.study.persist.Persist
import big.data.study.tools.StreamingContextFake
import org.scalatest.mock.MockitoSugar

import org.scalatest.{ShouldMatchers, WordSpec}
import big.data.study.fakes.ClockWrapper

import org.mockito.Matchers.argThat
import org.mockito.Mockito.verify

import org.scalatest.concurrent.Eventually
import org.scalatest.time.Millis
import org.scalatest.time.Span
import twitter4j.Status


class IngestorTest extends WordSpec  with MockitoSugar with ShouldMatchers with  Eventually {


  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(1200, Millis)))

  "When exist one element result" should {

    val sc = new StreamingContextFake[Status]("org.apache.spark.util.ManualClock")
    val clock = new ClockWrapper(sc)

    val persit = mock[Persist]

    new Ingestor(persit).ingest(sc.createDStrem)
    sc.start

    " be send one element to persist " in {



      sc.addDataInRDD(Seq(new StatusDouble("Real Madrid","MM/dd/yy","01/01/16")))
      clock.advance(1)
      eventually{
        val status = new StatusDouble("Real Madrid","MM/dd/yy","01/01/16")
        verify(persit).insert(argThat(new TupleMatcher(status.getText,status.getCreatedAt)))

        }

      sc.stop
    }

  }


}


