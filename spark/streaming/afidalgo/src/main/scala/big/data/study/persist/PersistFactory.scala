package big.data.study.persist


class PersistFactory(strategySeq:Seq[PersistStrategy]) {

  def getPersist (message:String): Seq[Persist] = {
    val regexWhiteSpace = "\\s+"
    val emptyChar = ""
    val joinMessageLowerCase = message.replaceAll(regexWhiteSpace, emptyChar).toLowerCase
    val coincidence =strategySeq.foldLeft(Seq.empty[PersistPriority])(
      (all,actual)=> actual.persistOf(joinMessageLowerCase,all)
    )
    coincidence
      .filter(p=> p.priority==coincidence.maxBy((persist)=>persist.priority).priority)
      .map(persistWithPriority => persistWithPriority.persist)
  }
}

object PersistFactory {

  private def seq = Seq(new PersistStrategy(PersistPriority(1,new WhiteTeam),"realmadrid"),
                        new PersistStrategy(PersistPriority(1,new BlueGarnetTeam),"barcelona"),
                        new PersistStrategy(PersistPriority(2,new AllTeamsPersist),"realmadrid","barcelona"),
                        new PersistStrategy(PersistPriority(0,new NotDefinedPersist)))

  def apply():PersistFactory={
      new PersistFactory(seq)
  }
}
