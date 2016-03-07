package big.data.study.doubles

import java.text.SimpleDateFormat
import java.util.Date

import twitter4j._


class StatusDouble(text:String,dateFormat:String,sDate:String) extends Status{

  override def getCreatedAt: Date = {
    val formatter = new SimpleDateFormat(dateFormat)
    formatter.parse(sDate)
  }

  override def getText: String = {
    text
  }

  override def getPlace: Place = ???
  override def isRetweet: Boolean = ???
  override def isFavorited: Boolean = ???
  override def getUser: User = ???
  override def getContributors: Array[Long] = ???
  override def getRetweetedStatus: Status = ???
  override def getInReplyToScreenName: String = ???
  override def isTruncated: Boolean = ???
  override def getId: Long = ???
  override def getCurrentUserRetweetId: Long = ???
  override def isPossiblySensitive: Boolean = ???
  override def getRetweetCount: Long = ???
  override def getGeoLocation: GeoLocation = ???
  override def getInReplyToUserId: Long = ???
  override def getSource: String = ???
  override def getInReplyToStatusId: Long = ???
  override def isRetweetedByMe: Boolean = ???
  override def compareTo(o: Status): Int = ???
  override def getAccessLevel: Int = ???
  override def getRateLimitStatus: RateLimitStatus = ???
  override def getHashtagEntities: Array[HashtagEntity] = ???
  override def getURLEntities: Array[URLEntity] = ???
  override def getMediaEntities: Array[MediaEntity] = ???
  override def getUserMentionEntities: Array[UserMentionEntity] = ???
}
