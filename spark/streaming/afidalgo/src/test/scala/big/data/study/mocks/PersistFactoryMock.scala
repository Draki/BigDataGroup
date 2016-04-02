package big.data.study.mocks

import big.data.study.persist.{Persist, PersistFactory}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.when


trait PersistFactoryMock extends MockitoSugar {

  def mockFactory (message:String,persist:Persist): PersistFactory = {
     val persistFactory = mock[PersistFactory]
     when(persistFactory.getPersist(message)).thenReturn(Seq(persist))
     persistFactory
  }

}
