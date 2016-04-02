package big.data.study.persist


class PersistStrategy (registered:PersistPriority,conditions:String*){

  def persistOf(message: String,last:Seq[PersistPriority]) : Seq[PersistPriority] =
    conditions.forall(condition => message.contains(condition)) match {
        case true => Seq(registered) ++ last
        case false =>  last
     }



}
