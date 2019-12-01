package kvs.lsm.behavior

import java.io.File

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import kvs.lsm.sstable.{SSTable, SSTableFactory}
import org.specs2.mutable.{After, Specification}
import org.specs2.specification.Scope

class SSTableMergeBehaviorSpec extends Specification {

  val classLoader = getClass.getClassLoader

  "Async" >> {

    "#Merge" should {

      trait context extends Scope with After {
        val sequenceNo = 12
        val testResourceDirectory =
          classLoader.getResource("test/sstable").getPath
        val testKit = ActorTestKit()
        val sSTableFactory = new SSTableFactory(3, testResourceDirectory)
        val mergeBehavior =
          testKit.spawn(SSTableMergeBehavior(sSTableFactory, 3))

        val probe = testKit.createTestProbe[SSTableMergeBehavior.Merged]()

        override def after: Any = {
          testKit.shutdownTestKit()
          val file = new File(
            classLoader
              .getResource("test/sstable")
              .getPath
              .concat(s"/segment_file_$sequenceNo.txt"))
          if (file.exists())
            file.delete()
        }
      }

      "return Merged" in new context {
        val sSTable2 = sSTableFactory.recovery(2)
        val sSTable3 = sSTableFactory.recovery(3)
        val sSTable4 = sSTableFactory.recovery(4)

        mergeBehavior ! SSTableMergeBehavior.Merge(sequenceNo,
                                                   Seq(sSTable2,
                                                       sSTable3,
                                                       sSTable4),
                                                   probe.ref)
        val merged = probe.receiveMessage()
        merged.removedSequenceNo === Seq(2, 3, 4)

        val getProbe = testKit.createTestProbe[SSTable.Got]()
        merged.mergedSegment.routerRef ! SSTableBehavior.Get("foo",
                                                             getProbe.ref)
        getProbe.receiveMessage() === SSTable.Got.Found("fooValue")
        merged.mergedSegment.routerRef ! SSTableBehavior.Get("gar",
                                                             getProbe.ref)
        getProbe.receiveMessage() === SSTable.Got.NotFound
        merged.mergedSegment.routerRef ! SSTableBehavior.Get("key1",
                                                             getProbe.ref)
        getProbe.receiveMessage() === SSTable.Got.Deleted
      }

    }

  }

}
