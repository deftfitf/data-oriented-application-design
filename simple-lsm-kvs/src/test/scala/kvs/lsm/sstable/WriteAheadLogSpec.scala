package kvs.lsm.sstable

import java.io.File
import java.nio.file.{Files, Paths}

import kvs.lsm.sstable.SSTable.Value
import org.specs2.mutable.{After, Specification}
import org.specs2.specification.Scope

class WriteAheadLogSpec extends Specification {

  val classLoader = getClass.getClassLoader

  "WriteAheadLog" should {

    trait case1 extends Scope with After {
      val expectedWriteAheadLog =
        Files.readAllBytes(
          Paths.get(
            classLoader.getResource("test/wal/write_ahead_log_test.txt").toURI))

      val writeAheadLog =
        WriteAheadLog.initialize(
          classLoader
            .getResource("test/wal")
            .getPath
            .concat("/write_ahead_log_test_temp.txt"))

      def after = {
        writeAheadLog.close()
        val file = new File(
          classLoader
            .getResource("test/wal")
            .getPath
            .concat("/write_ahead_log_test_temp.txt"))
        if (file.exists())
          file.delete()
      }
    }

    "write Set or Del to a file in binary format" in new case1 {
      writeAheadLog.set("key1", "value1")
      writeAheadLog.set("key2", "value2")
      writeAheadLog.set("key3", "value3")
      writeAheadLog.set("key2", "value2'")
      writeAheadLog.set("key1", "value1'")
      writeAheadLog.del("key2")
      writeAheadLog.set("key1", "value1''")

      val writtenLog =
        Files.readAllBytes(
          Paths.get(
            classLoader
              .getResource("test/wal/write_ahead_log_test_temp.txt")
              .toURI))

      writtenLog.sameElements(expectedWriteAheadLog) must beTrue
    }

    trait case2 extends Scope with After {
      val writeAheadLog =
        WriteAheadLog.initialize(
          classLoader.getResource("test/wal/write_ahead_log_test.txt").getPath)

      def after: Any = {
        writeAheadLog.close()
      }
    }

    "a file written in binary format can be replayed correctly" in new case2 {
      writeAheadLog.recovery().iterator.toList ===
        List(("key1", Value.Exist("value1''")),
             ("key2", Value.Deleted),
             ("key3", Value.Exist("value3")))
    }

  }

}
