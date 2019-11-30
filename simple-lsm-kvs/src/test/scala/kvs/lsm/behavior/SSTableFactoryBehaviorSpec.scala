package kvs.lsm.behavior

import java.io.File

import akka.actor.testkit.typed.scaladsl.{
  ActorTestKit,
  BehaviorTestKit,
  TestInbox
}
import kvs.lsm.sstable.Log.MemTable
import kvs.lsm.sstable.{SSTable, SSTableFactory, SegmentFile, SparseKeyIndex}
import org.specs2.mutable.{After, Specification}
import org.specs2.specification.Scope

import scala.collection.mutable

class SSTableFactoryBehaviorSpec extends Specification {

  val classLoader: ClassLoader = getClass.getClassLoader

  "Sync" >> {

    trait context extends Scope {
      val testResourceDirectory =
        classLoader.getResource("test/sstable").getPath
      val sSTableFactory = new SSTableFactory(3, testResourceDirectory)
      val testKit = BehaviorTestKit(SSTableFactoryBehavior(sSTableFactory, 1))
      val inbox = TestInbox[SSTableFactoryBehavior.Applied]()
    }

    "#Initialize" should {

      "return Applied" in new context {
        testKit.run(SSTableFactoryBehavior.Initialize(2, inbox.ref))

        val expectedSparseKeys = IndexedSeq("bar", "key1", "key_3")
        val expectedPositions = IndexedSeq(0L, 50L, 98L)
        val expectedSparseKeyIndex =
          SparseKeyIndex(expectedSparseKeys, expectedPositions)
        val expectedSegmentFile = SegmentFile(2, testResourceDirectory)
        val expectSSTable = SSTable(expectedSegmentFile, expectedSparseKeyIndex)

        val applied = inbox.receiveMessage()
        applied.sSTableRef.sSTable === expectSSTable
      }

    }

    "#Apply" should {

      "return Applied" in new context with After {
        val sequenceNo = 10
        val memTable =
          MemTable(
            mutable.TreeMap[String, SSTable.Value](
              ("bar", SSTable.Value.Deleted),
              ("baz", SSTable.Value.Exist("bazValue")),
              ("foo", SSTable.Value.Exist("fooValue")),
              ("key1", SSTable.Value.Deleted),
              ("key2", SSTable.Value.Exist("0040999494")),
              ("key3", SSTable.Value.Exist("93948848")),
              ("key4", SSTable.Value.Exist("l30ls@@zsds"))
            ))

        testKit.run(
          SSTableFactoryBehavior.Apply(sequenceNo, memTable, inbox.ref))

        val expectedSparseKeys = IndexedSeq("bar", "key1", "key4")
        val expectedPositions = IndexedSeq(0L, 49L, 103L)
        val expectedSparseKeyIndex =
          SparseKeyIndex(expectedSparseKeys, expectedPositions)
        val expectedSegmentFile = SegmentFile(sequenceNo, testResourceDirectory)
        val expectSSTable = SSTable(expectedSegmentFile, expectedSparseKeyIndex)

        val applied = inbox.receiveMessage()
        applied.sSTableRef.sSTable === expectSSTable

        override def after: Any = {
          val file = new File(
            classLoader
              .getResource("test/sstable")
              .getPath
              .concat(s"/segment_file_$sequenceNo.txt"))
          if (file.exists())
            file.delete()
        }
      }

    }

  }

  "Async" >> {

    trait context extends Scope with After {
      val testResourceDirectory =
        classLoader.getResource("test/sstable").getPath
      val testKit = ActorTestKit()
      val sSTableFactory = new SSTableFactory(3, testResourceDirectory)
      val factoryBehavior =
        testKit.spawn(SSTableFactoryBehavior(sSTableFactory, 1))
      val probe = testKit.createTestProbe[SSTableFactoryBehavior.Applied]()

      override def after: Any = {
        testKit.shutdownTestKit()
      }
    }

    "#Initialize" should {

      "return Applied" in new context {
        factoryBehavior ! SSTableFactoryBehavior.Initialize(2, probe.ref)

        val applied = probe.receiveMessage()

        val getProbe = testKit.createTestProbe[SSTable.Got]()
        applied.sSTableRef.routerRef ! SSTableBehavior.Get("key1", getProbe.ref)
        getProbe.receiveMessage() === SSTable.Got.Found("value1")
      }

    }

    "#Apply" should {

      "return Applied" in new context {
        val sequenceNo = 11
        val memTable =
          MemTable(
            mutable.TreeMap[String, SSTable.Value](
              ("bar", SSTable.Value.Deleted),
              ("baz", SSTable.Value.Exist("bazValue")),
              ("foo", SSTable.Value.Exist("fooValue")),
              ("key1", SSTable.Value.Deleted),
              ("key2", SSTable.Value.Exist("0040999494")),
              ("key3", SSTable.Value.Exist("93948848")),
              ("key4", SSTable.Value.Exist("l30ls@@zsds"))
            ))

        factoryBehavior ! SSTableFactoryBehavior.Apply(sequenceNo,
                                                       memTable,
                                                       probe.ref)

        val applied = probe.receiveMessage()

        val getProbe = testKit.createTestProbe[SSTable.Got]()
        applied.sSTableRef.routerRef ! SSTableBehavior.Get("key1", getProbe.ref)
        applied.sSTableRef.routerRef ! SSTableBehavior.Get("key1_",
                                                           getProbe.ref)
        applied.sSTableRef.routerRef ! SSTableBehavior.Get("key2", getProbe.ref)
        getProbe.receiveMessages(3) ===
          Seq(SSTable.Got.Deleted,
              SSTable.Got.NotFound,
              SSTable.Got.Found("0040999494"))

        override def after: Any = {
          super.after
          val file = new File(
            classLoader
              .getResource("test/sstable")
              .getPath
              .concat(s"/segment_file_$sequenceNo.txt"))
          if (file.exists())
            file.delete()
        }
      }

    }

  }

}
