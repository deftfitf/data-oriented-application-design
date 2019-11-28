package kvs.lsm.sstable

import java.io.RandomAccessFile

import kvs.lsm.sstable.SSTable.Value
import kvs.lsm.sstable.SSTableFactory._

import scala.annotation.tailrec
import scala.collection.mutable

class SSTableFactory(sparseIndexPer: Int, segmentFileBathPath: String) {

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
      val segmentFile = new SegmentFile(sequenceNo, segmentFileBathPath)
      segmentFile.createSegmentFile()
      val raf = segmentFile.newReadWriteRaf()

      val keyIndex = mutable.ArrayBuffer[String]()
      val positionIndex = mutable.ArrayBuffer[Long]()

      @tailrec
      def loop(idx: Int): Unit =
        if (iterator.hasNext) {
          val currentPosition = raf.getFilePointer
          val (key, value) = iterator.next()
          writeKeyValue(key, value, raf)

          if (idx % sparseIndexPer == 0) {
            keyIndex.addOne(key)
            positionIndex.addOne(currentPosition)
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
      val segmentFile = new SegmentFile(sequenceNo, segmentFileBathPath)
      val reader = new SSTableReader(segmentFile.newReadWriteRaf())
      try {
        val keyIndex = mutable.ArrayBuffer[String]()
        val positionIndex = mutable.ArrayBuffer[Long]()

        def pickUpKeys(): Unit = {
          @tailrec
          def recursive(idx: Int): Unit =
            if (reader.hasNext) {
              if (idx % sparseIndexPer == 0) {
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

  private class SSTableReader(
      override protected val readOnlyRaf: RandomAccessFile)
      extends SegmentFileReadable

}
