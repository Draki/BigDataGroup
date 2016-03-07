package big.data.study

import org.scalatest.WordSpec



/**
 * Test to twiterIngestor
 */
class TwiterIngestorTest extends WordSpec {


  "first test with twiter " should  {

          "first call " in {
              new TwitterIngestor().ingestTwiterTags()
          }
    }

}
