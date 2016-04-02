package big.data.study.persist

import big.data.study.doubles.{DefaultPersist, RegisteredPersist}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ShouldMatchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class PersistStrategyTest extends WordSpec with ShouldMatchers{

  private val firstCondition ="first"
  private val secondCondition = "second"
  private val strategy = new PersistStrategy(PersistPriority(0,new RegisteredPersist), firstCondition,secondCondition)


  "We can concatenate Persist and result " should {
    val existPersist = Seq(PersistPriority(0,new DefaultPersist))

     " be sequence with registered persist and exist persist when complain all conditions" in {

         val expectedLength = 2
         val persistSeq = strategy.persistOf(firstCondition.concat(secondCondition),existPersist)
         persistSeq.size shouldBe  expectedLength
     }

    " be sequence with exist persist when only complain one condition " in  {
      val expectedLength = 1
      val persistSeq = strategy.persistOf(firstCondition,existPersist)
      persistSeq.size shouldBe expectedLength
    }

     " be sequence with exist persist when  not complain all conditions " in  {
        val notMatch = "notMatch"
       val expectedLength = 1
       val persistSeq = strategy.persistOf(notMatch,existPersist)
       persistSeq.size shouldBe expectedLength
     }
  }



}
