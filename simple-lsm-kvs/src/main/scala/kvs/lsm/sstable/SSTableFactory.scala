package kvs.lsm.sstable

import java.io.RandomAccessFile

import kvs.lsm.sstable.SSTable.Value
import kvs.lsm.sstable.SSTableFactory._

import scala.annotation.tailrec
import scala.collection.mutable

class SSTableFactory {

  @throws[java.io.IOException]
  private def writeKeyValue(key: String,
                            value: Value,
                            raf: RandomAccessFile): Unit = {
    raf.writeInt(key.length)
    raf.writeBytes(key)
    value match {
      case Value.Exist(v) =>
        raf.writeInt(v.length)
        raf.writeBytes(v)
      case Value.Deleted =>
        raf.writeInt(SegmentFile.TOMBSTONE)
    }
  }

  @throws[SSTableInitializeError]
  def apply(sequenceNo: Int, iterator: Iterator[(String, Value)]): SSTable =
    try {
      val segmentFile = new SegmentFile(sequenceNo)
      segmentFile.createSegmentFile()
      val raf = segmentFile.newReadWriteRaf()

      val keyIndex = mutable.ArrayBuffer[String]()
      val positionIndex = mutable.ArrayBuffer[Long]()

      @tailrec
      def loop(idx: Int): Unit =
        if (iterator.hasNext) {
          val (key, value) = iterator.next()
          writeKeyValue(key, value, raf)

          if (idx % SPARSE_INDEX_PER == 0) {
            keyIndex.addOne(key)
            positionIndex.addOne(raf.getFilePointer)
          }
          loop(idx + 1)
        }

      try {
        loop(0)
      } finally {
        raf.close()
      }

      val sparseKeyIndex =
        SparseKeyIndex(keyIndex.toIndexedSeq, positionIndex.toIndexedSeq)
      new SSTable(segmentFile, sparseKeyIndex)
    } catch {
      case e: Throwable => throw SSTableInitializeError(e.getMessage)
    }

  @throws[SSTableRecoveryError]
  def recovery(sequenceNo: Int): SSTable =
    try {
      val segmentFile = new SegmentFile(sequenceNo)
      val reader = new SSTableReader(segmentFile.newReadWriteRaf())
      try {
        val keyIndex = mutable.ArrayBuffer[String]()
        val positionIndex = mutable.ArrayBuffer[Long]()

        def pickUpKeys(): Unit = {
          @tailrec
          def recursive(idx: Int): Unit =
            if (reader.hasNext) {
              if (idx % SPARSE_INDEX_PER == 0) {
                val pointer = reader.currentPointer()
                val key = reader.readKey()
                keyIndex.addOne(new String(key))
                positionIndex.addOne(pointer)
              } else {
                reader.skipKey()
              }
              reader.skipValue()
              recursive(idx + 1)
            }
          recursive(0)
        }
        pickUpKeys()

        val sparseKeyIndex =
          SparseKeyIndex(keyIndex.toIndexedSeq, positionIndex.toIndexedSeq)
        new SSTable(segmentFile, sparseKeyIndex)
      } finally {
        reader.close()
      }
    } catch {
      case e: Throwable => throw SSTableRecoveryError(e.getMessage)
    }

}

object SSTableFactory {

  final case class SSTableInitializeError(message: String)
      extends Throwable(message)
  final case class SSTableRecoveryError(message: String)
      extends Throwable(message)

  final private val SPARSE_INDEX_PER = 100

  private class SSTableReader(
      override protected val readOnlyRaf: RandomAccessFile)
      extends SegmentFileReadable

}
