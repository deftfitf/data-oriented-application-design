package kvs.lsm.sstable

import kvs.lsm.sstable.SSTable.{Got, SSTableReader}
import org.specs2.mutable.{After, Specification}
import org.specs2.specification.Scope

class SSTableReaderSpec extends Specification {

  val classLoader: ClassLoader = getClass.getClassLoader

  "#get" should {

    trait context extends Scope with After {
      private val testResourceDirectory =
        classLoader.getResource("test/sstable").getPath
      private val sSTableFactory = new SSTableFactory(2, testResourceDirectory)
      private val sSTable = sSTableFactory.recovery(4)
      val reader: SSTableReader = sSTable.newReader()

      override def after: Any = {
        reader.close()
      }
    }

    "return Got.Found(value) when found value of input key" >> {

      "when SSTable has index key" in new context {
        reader.get("foo") must beEqualTo(Got.Found("fooValue"))
        reader.get("key2") must beEqualTo(Got.Found("0040999494"))
      }

      "when SSTable has not index key" in new context {
        reader.get("baz") must beEqualTo(Got.Found("bazValue"))
        reader.get("key3") must beEqualTo(Got.Found("93948848"))
      }

    }

    "return Got.NotFound when not found of input key" >> {

      "when key is ahead of first key in SSTable" in new context {
        reader.get("aar") must beEqualTo(Got.NotFound)
      }

      "when key is between 2 keys in SSTable" in new context {
        reader.get("car") must beEqualTo(Got.NotFound)
      }

      "ehen key is after of last key in SSTable" in new context {
        reader.get("xxr") must beEqualTo(Got.NotFound)
      }

    }

    "return Got.Deleted when value of input key was deleted" in new context {

      "when SSTable has index key" in new context {
        reader.get("bar") must beEqualTo(Got.Deleted)
      }

      "when SSTable has not index key" in new context {
        reader.get("key1") must beEqualTo(Got.Deleted)
      }

    }

  }

}
