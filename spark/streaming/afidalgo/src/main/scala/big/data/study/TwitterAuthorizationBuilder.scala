package big.data.study

import com.typesafe.config.Config
import twitter4j.{Twitter, TwitterFactory}



class TwitterAuthorizationBuilder (config: Config){

  private lazy val configuration =  new twitter4j.conf.ConfigurationBuilder()
                          .setOAuthConsumerKey(config.getString("authKeys.consumerKey").trim)
                          .setOAuthConsumerSecret(config.getString("authKeys.consumerSecret").trim)
                          .setOAuthAccessToken(config.getString("authKeys.accessToken").trim)
                          .setOAuthAccessTokenSecret(config.getString("authKeys.tokenSecret".trim))
                          .build

  def build() : Twitter = {
   val authorization =  new twitter4j.auth.OAuthAuthorization(configuration)
   val twitter_auth = new TwitterFactory(configuration)
   twitter_auth.getInstance(authorization)
  }
}



