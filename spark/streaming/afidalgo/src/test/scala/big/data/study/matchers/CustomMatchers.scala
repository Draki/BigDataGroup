package big.data.study.matchers

import java.util.Date

import org.mockito.ArgumentMatcher


class StringMatcher(argument: String) extends ArgumentMatcher[String] {

  override def matches(arg: scala.Any): Boolean = {
    val stringArg = arg.asInstanceOf[String]
    val result = stringArg.eq(argument)
    result
  }
}


class DateMatcher (argument: Date)  extends ArgumentMatcher[Date] {

  override def matches(arg: scala.Any): Boolean = {
     val date = arg.asInstanceOf[Date]
     val result = date.getTime == argument.getTime
    result
  }
}

class TupleMatcher(argument:(String,Date)) extends ArgumentMatcher[(String,Date)]{

  override def matches(arg: scala.Any): Boolean = {
    val tuple = arg.asInstanceOf[Tuple2[String,Date]]
    val eqDate = tuple._2.getTime == argument._2.getTime
    val eqString =  tuple._1 == argument._1
    eqDate && eqString
  }
}
