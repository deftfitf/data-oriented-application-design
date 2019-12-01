package kvs.lsm.sstable

import java.io.File
import java.nio.file.{Files, Paths}

import org.specs2.mutable.{After, Specification}
import org.specs2.specification.Scope

import scala.collection.immutable.TreeMap

class SSTableFactorySpec extends Specification {

  val classLoader = getClass.getClassLoader

  "#apply,#recovery" should {

    trait context extends Scope with After {
      val expectedSSTableBinary =
        Files.readAllBytes(
          Paths.get(
            classLoader
              .getResource("test/sstable/segment_file_1_test.txt")
              .toURI))

      val currentSequenceNo = 1
      val testResourceDirectory =
        classLoader.getResource("test/sstable").getPath
      val sSTableFactory = new SSTableFactory(3, testResourceDirectory)

      override def after = {
        val file = new File(
          classLoader
            .getResource("test/sstable")
            .getPath
            .concat(s"/segment_file_$currentSequenceNo.txt"))
        if (file.exists())
          file.delete()
      }
    }

    "persist SSTable and recover from persistent SSTable" in new context {
      val writeIterator =
        TreeMap[String, SSTable.Value](
          ("bar", SSTable.Value.Deleted),
          ("baz", SSTable.Value.Exist("bazValue")),
          ("foo", SSTable.Value.Exist("fooValue")),
          ("key1", SSTable.Value.Deleted),
          ("key2", SSTable.Value.Exist("0040999494")),
          ("key3", SSTable.Value.Exist("93948848")),
          ("key4", SSTable.Value.Exist("l30ls@@zsds"))
        ).iterator

      val sSTable = sSTableFactory.apply(currentSequenceNo, writeIterator)

      val expectedSparseKeys = IndexedSeq("bar", "key1", "key4")
      val expectedPositions = IndexedSeq(0L, 49L, 103L)
      sSTable.sparseKeyIndex === SparseKeyIndex(expectedSparseKeys,
                                                expectedPositions)

      val sSTableBinary =
        Files.readAllBytes(
          Paths.get(
            classLoader.getResource("test/sstable/segment_file_1.txt").toURI))
      sSTableBinary.sameElements(expectedSSTableBinary) must beTrue

      val recoveredSSTable = sSTableFactory.recovery(currentSequenceNo)
      sSTable.sparseKeyIndex === recoveredSSTable.sparseKeyIndex
    }

  }

}
