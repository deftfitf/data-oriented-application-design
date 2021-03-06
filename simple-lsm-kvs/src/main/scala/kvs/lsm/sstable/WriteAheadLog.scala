package kvs.lsm.sstable

import java.io.{File, RandomAccessFile}

import kvs.lsm.sstable.Log.MemTable
import kvs.lsm.sstable.SSTable.Value
import kvs.lsm.sstable.WriteAheadLog.WriteAheadLogRecoveryError

import scala.annotation.tailrec
import scala.collection.mutable

class WriteAheadLog(raf: RandomAccessFile) extends SegmentFileReadable {

  override protected val readOnlyRaf: RandomAccessFile = raf

  def set(key: String, value: String): Unit = {
    raf.writeInt(key.length)
    raf.writeBytes(key)
    raf.writeInt(value.length)
    raf.writeBytes(value)
  }

  def del(key: String): Unit = {
    raf.writeInt(key.length)
    raf.writeBytes(key)
    raf.writeInt(SegmentFile.TOMBSTONE)
  }

  @throws[WriteAheadLogRecoveryError]
  def recovery(): MemTable =
    try {
      val treeMap = mutable.TreeMap[String, Value]()
      @tailrec
      def loop(): Unit =
        if (hasNext) {
          val key = readKey()
          val value = readValue()
          treeMap.update(key, value)
          loop()
        }
      loop()
      new MemTable(treeMap)
    } catch {
      case e: Throwable =>
        throw WriteAheadLogRecoveryError(e.getMessage)
    }

  def clear(): Unit = raf.setLength(0)

}

object WriteAheadLog {

  final case class WriteAheadLogRecoveryError(message: String)
      extends Throwable(message)
  final case class WriteAheadLogInitializeError(message: String)
      extends Throwable(message)

  @throws[WriteAheadLogInitializeError]
  def initialize(writeAheadLogPath: String): WriteAheadLog = {
    val file = new File(writeAheadLogPath)
    if (!file.exists() && !file.createNewFile()) {
      throw WriteAheadLogInitializeError(
        s"can't find and create write ahead log: $writeAheadLogPath")
    }
    try {
      val raf = new RandomAccessFile(file, "rw")
      new WriteAheadLog(raf)
    } catch {
      case e: java.io.FileNotFoundException =>
        throw WriteAheadLogInitializeError(e.getMessage)
    }
  }

}
