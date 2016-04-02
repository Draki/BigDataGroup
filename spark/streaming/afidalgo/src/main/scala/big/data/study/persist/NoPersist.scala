package big.data.study.persist

import java.util.Date


class NoPersist extends Persist {


  override def insert(tuple: (String, Date)): Unit = {

    println("%s : ".format(tuple._1))
  }
}
