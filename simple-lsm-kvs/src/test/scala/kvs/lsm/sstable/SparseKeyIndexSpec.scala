package kvs.lsm.sstable

import kvs.lsm.sstable.SparseKeyIndex.Position
import org.specs2.mutable.Specification
import org.specs2.specification.Scope

class SparseKeyIndexSpec extends Specification {

  "#positionRange" should {

    trait context extends Scope {
      val index = SparseKeyIndex(
        IndexedSeq("bar", "baz", "foo", "key1", "key2", "key3", "key4"),
        IndexedSeq(84L, 95L, 194L, 345L, 442L, 809L, 1204L))
    }

    "return NotFound if the key you are trying to find is smaller than the smallest key you have" in new context {
      index.positionRange("aar") must beEqualTo(Position.NotFound)
    }

    "return Found if the key exists with confirmation" in new context {
      index.positionRange("foo") must beEqualTo(Position.Found(194L))
      index.positionRange("bar") must beEqualTo(Position.Found(84L))
      index.positionRange("key4") must beEqualTo(Position.Found(1204L))
    }

    "return Range when the start position is the last key held in this index" in new context {
      index.positionRange("car") must beEqualTo(Position.Range(95L, 194L))
      index.positionRange("key1z") must beEqualTo(Position.Range(345L, 442L))
    }

    "return Tail when the key you are trying to find is larger than the largest key it holds" in new context {
      index.positionRange("zey") must beEqualTo(Position.Tail(1204L))
      index.positionRange("key4a") must beEqualTo(Position.Tail(1204L))
    }

  }

}
