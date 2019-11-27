package kvs.lsm.sstable

import java.io.RandomAccessFile

import kvs.lsm.sstable.SSTable.Got.NotFound
import kvs.lsm.sstable.SSTable._

import scala.annotation.tailrec

case class SSTable private[sstable] (segmentFile: SegmentFile,
                                     sparseKeyIndex: SparseKeyIndex) {

  def sequenceNo: Int = segmentFile.sequenceNo

  def newReader(): SSTableReader = {
    val readOnlyRaf = segmentFile.newReadOnlyRaf()
    new SSTableReader(readOnlyRaf, sparseKeyIndex)
  }

}

object SSTable {

  class SSTableReader(override protected val readOnlyRaf: RandomAccessFile,
                      sparseKeyIndex: SparseKeyIndex)
      extends SegmentFileReadable {

    /**
      * keyBytesに対応するキーの値を [start, end) の区間探す
      * @param keyBytes 対象キー
      * @param start 開始位置(含む)
      * @param end 終了位置(含まない)
      * @throws
      * @return
      */
    @throws[java.io.IOException]
    private def rangeSearch(keyBytes: Array[Byte],
                            start: Long,
                            end: Long): Got = {
      @tailrec
      def loop(): Got =
        if (readOnlyRaf.getFilePointer < end) {
          val currentKey = readKeyBytes()
          if (currentKey.sameElements(keyBytes)) {
            readValue().toGot
          } else {
            skipValue()
            loop()
          }
        } else {
          NotFound
        }

      readOnlyRaf.seek(start)
      loop()
    }

    @throws[java.io.IOException]
    def get(key: String): Got =
      sparseKeyIndex.positionRange(key) match {
        case (left, Some(right)) => rangeSearch(key.getBytes, left, right)
        case (left, None)        => rangeSearch(key.getBytes, left, RAF_LENGTH)
      }

  }

  sealed trait Got

  object Got {
    final case class Found(value: String) extends Got
    final case object NotFound extends Got
    final case object Deleted extends Got
  }

  sealed trait Value {
    def toGot: Got
  }

  object Value {

    final case class Exist(value: String) extends Value {
      override def toGot: Got = Got.Found(value)
    }

    final case object Deleted extends Value {
      override def toGot: Got = Got.Deleted
    }

  }

}
