package big.data.study.persist

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ShouldMatchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class PersistFactoryTest extends WordSpec with ShouldMatchers{


  private val factory = PersistFactory()
  private  val firstElement = 0

  " When text contain only element search result " should {


      " be WhiteTeamPersist instance when is Real Madrid " in {
        val message = "Real Madrid"
        factory.getPersist(message)(firstElement) shouldBe an[WhiteTeam]
      }

      " be BlueGarnetTeam instance when is Barcelona " in  {
        factory.getPersist("Barcelona")(firstElement) shouldBe an[BlueGarnetTeam]
      }

      " be WhiteTeamPersist instance when is Real madrid " in {
        val message = "Real madrid"
        factory.getPersist(message)(firstElement) shouldBe an[WhiteTeam]
      }

      " be WhiteTeamPersist instance when is Real     Madrid" in  {
        val message = "Real     Madrid"
        factory.getPersist(message)(firstElement) shouldBe an[WhiteTeam]
      }
  }

  " When text contain more elements then searcher result " should {

      " be WhiteTeamPersist instance when is In Real Madrid More" in {
        val message = "In Real Madrid More"
        factory.getPersist(message)(firstElement) shouldBe an[WhiteTeam]
      }

      " be AllTeamsPersist instance when is Real Madrid versus Barcelona very good" in {
        val message = "Real Madrid versus Barcelona very good"
        factory.getPersist(message)(firstElement) shouldBe an[AllTeamsPersist]
      }

      " Be NotDefinedPersist when instance is any word not reistered " in {
        val message ="any word not reistered "
        factory.getPersist(message)(firstElement) shouldBe an[NotDefinedPersist]
      }
  }

}
