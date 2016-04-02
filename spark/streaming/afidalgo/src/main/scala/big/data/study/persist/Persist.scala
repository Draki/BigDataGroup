package big.data.study.persist

import java.util.Date

trait Persist{


  def insert(tuple:(String,Date)) : Unit

}


