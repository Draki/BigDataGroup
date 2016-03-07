package big.data.study.persist

import java.util.Date

trait Persist {


  def insert(tuple:(String,Date)) : Unit

}

class NoPersist extends Persist {


  override def insert(tuple: (String, Date)): Unit = {

    println("%s : ".format(tuple._1))
  }
}

