package big.data.study

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ShouldMatchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class AuthAuthorizationBuilderTest extends WordSpec with ShouldMatchers {

  trait Data {

    val authorization = new TwitterAuthorizationBuilder(config = ConfigFactory.parseString(
                                        """authKeys {
                                          |          consumerKey= a
                                          |          consumerSecret = b
                                          |          accessToken = accessToken
                                          |          tokenSecret = secretToken
                                           }
                                          |
                                        """.stripMargin
                                      )).build()
  }

  "When build twiter Authorization result " should {


    " Authorization  with configrable token" in new Data{

      authorization.getOAuthAccessToken.getToken shouldBe  "accessToken"
    }

    " Authorization with configurable Secret Key " in new Data {

      authorization.getOAuthAccessToken.getTokenSecret shouldBe  "secretToken"
    }
  }

}
