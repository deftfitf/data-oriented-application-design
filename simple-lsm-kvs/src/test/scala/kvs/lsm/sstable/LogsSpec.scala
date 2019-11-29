package kvs.lsm.sstable

import kvs.lsm.sstable.Log.{MemTable, SSTableRef}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.specification.Scope

class LogsSpec extends Specification with Mockito {

  trait base extends Scope {
    val memTable1, memTable2 = mock[MemTable]
    val sSTableRef1, sSTableRef2, sSTableRef3, sSTableRef4 = mock[SSTableRef]
    val sSTable1, sSTable2, sSTable3, sSTable4 = mock[SSTable]

    sSTableRef1.sSTable returns sSTable1
    sSTableRef2.sSTable returns sSTable2
    sSTableRef3.sSTable returns sSTable3
    sSTableRef4.sSTable returns sSTable4

    sSTable1.sequenceNo returns 2
    sSTable2.sequenceNo returns 5
    sSTable3.sequenceNo returns 7
    sSTable4.sequenceNo returns 9
  }

  "#activeSequenceNo" should {

    "return SSTable sequence no, in reverse order" in new base {
      Logs.empty
        .updated(2, sSTableRef1)
        .updated(5, sSTableRef2)
        .updated(7, sSTableRef3)
        .updated(8, memTable1)
        .updated(9, sSTableRef4)
        .updated(13, memTable2)
        .activeSequenceNo === Seq(9, 7, 5, 2)
    }

  }

  "#merged" should {

    "return updated Logs, removed old SSTable, add new merged SSTable" in new base {
      val mergedSSTableRef = mock[SSTableRef]
      val mergedSSTable = mock[SSTable]
      mergedSSTableRef.sSTable returns mergedSSTable
      mergedSSTable.sequenceNo returns 10

      Logs.empty
        .updated(2, sSTableRef1)
        .updated(4, memTable1)
        .updated(5, sSTableRef2)
        .updated(7, sSTableRef3)
        .updated(9, sSTableRef4)
        .merged(Seq(7, 9), mergedSSTableRef)
        .activeSequenceNo === Seq(10, 5, 2)

      Logs.empty
        .updated(2, sSTableRef1)
        .updated(4, memTable1)
        .updated(5, sSTableRef2)
        .updated(7, sSTableRef3)
        .updated(9, sSTableRef4)
        .merged(Seq(5, 7), mergedSSTableRef)
        .activeSequenceNo === Seq(10, 9, 2)
    }

  }

  "#mergeableSSTables" should {

    "returns mergeable SSTable" in new base {
      Logs.empty
        .updated(2, sSTableRef1)
        .updated(4, memTable1)
        .updated(5, sSTableRef2)
        .updated(7, sSTableRef3)
        .updated(9, sSTableRef4)
        .mergeableSSTables must beSome(Seq(sSTable4, sSTable3, sSTable2))

      Logs.empty
        .updated(2, sSTableRef1)
        .updated(3, sSTableRef3)
        .updated(4, memTable1)
        .updated(5, sSTableRef2)
        .updated(9, sSTableRef4)
        .mergeableSSTables must beSome(Seq(sSTable4, sSTable2))
    }

    "returns None if mergeable SSTable only one" in new base {
      Logs.empty
        .updated(2, sSTableRef1)
        .updated(4, memTable1)
        .updated(5, sSTableRef2)
        .updated(6, memTable1)
        .updated(7, sSTableRef3)
        .mergeableSSTables must beNone
    }

    "returns None latest is MemTable" in new base {
      Logs.empty
        .updated(2, sSTableRef1)
        .updated(4, memTable1)
        .updated(5, sSTableRef2)
        .updated(7, sSTableRef3)
        .updated(8, memTable1)
        .mergeableSSTables must beNone
    }

  }

}
