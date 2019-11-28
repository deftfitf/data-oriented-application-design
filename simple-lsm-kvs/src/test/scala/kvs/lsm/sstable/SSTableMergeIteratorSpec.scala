package kvs.lsm.sstable

import org.specs2.mutable.Specification
import org.specs2.specification.Scope

import scala.collection.immutable.TreeMap

class SSTableMergeIteratorSpec extends Specification {

  val classLoader = getClass.getClassLoader

  "#apply" should {

    trait context extends Scope {
      val nextSequenceNo = 5
      val testResourceDirectory =
        classLoader.getResource("test/sstable").getPath
      val sSTableFactory = new SSTableFactory(100, testResourceDirectory)

      val sSTable2 = sSTableFactory.recovery(2)
      val sSTable3 = sSTableFactory.recovery(3)
      val sSTable4 = sSTableFactory.recovery(4)
    }

    "return merged SSTable Iterator" in new context {
      SSTableMergeIterator(Seq(sSTable2, sSTable3, sSTable4)).toList ===
        TreeMap[String, SSTable.Value](
          ("bar", SSTable.Value.Deleted),
          ("baz", SSTable.Value.Exist("bazValue")),
          ("foo", SSTable.Value.Exist("fooValue")),
          ("foo_", SSTable.Value.Exist("fooVa?")),
          ("key1", SSTable.Value.Deleted),
          ("key1_2", SSTable.Value.Deleted),
          ("key2", SSTable.Value.Exist("0040999494")),
          ("key2_1", SSTable.Value.Exist("efffsd")),
          ("key3", SSTable.Value.Exist("93948848")),
          ("key_3", SSTable.Value.Exist("value_V3")),
          ("keay4", SSTable.Value.Exist("l3dd0ls@@zsds")),
          ("key4", SSTable.Value.Exist("l30ls@@zsds")),
          ("key4.5", SSTable.Value.Exist("dddd"))
        ).iterator.toList
    }

    "sort and merge in ascending order of segment number (newest first)" in new context {
      SSTableMergeIterator(Seq(sSTable4, sSTable3, sSTable2)).toList ===
        SSTableMergeIterator(Seq(sSTable2, sSTable3, sSTable4)).toList
    }

  }

}
