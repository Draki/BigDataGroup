package big.data.study.persist

import java.util.Date


class NotDefinedPersist extends Persist{
  override def insert(tuple: (String, Date)): Unit = {}
}
