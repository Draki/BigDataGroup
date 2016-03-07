package big.data.study.matchers

import java.util.Date

import org.mockito.ArgumentMatcher


class TupleMatcher(argument:(String,Date)) extends ArgumentMatcher[(String,Date)]{

  override def matches(arg: scala.Any): Boolean = {
    val tuple = arg.asInstanceOf[(String,Date)]
    val eqDate = tuple._2.getTime == argument._2.getTime
    val eqString =  tuple._1 == argument._1
    eqDate && eqString
  }
}
