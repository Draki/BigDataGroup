package big.data.study.doubles

import java.util.Date

import big.data.study.persist.Persist


class DefaultPersist extends Persist{
  override def insert(tuple: (String, Date)): Unit = {}
}
