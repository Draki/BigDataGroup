package big.data.study

import org.scalatest.WordSpec



class TwiterIngestorTest extends WordSpec {


  "first test with twiter " should  {

          "first call " in {
              new TwitterIngestor().ingestTwiterTags()
          }
    }

}
