package kvs.lsm.sstable

import kvs.lsm.sstable.SSTable.{SSTableReader, Value}

import scala.annotation.tailrec

/**
  * @param sSTables 新しい順に並んだSSTableのReaderのIterator
  */
class SSTableMergeIterator private (sSTables: IndexedSeq[SSTableReader])
    extends Iterator[(String, Value)] {

  private val BUFFER_IS_EMPTY = 0
  private val BUFFER_IS_FILLED = 1
  private val REACHED_EOF = 2

  private val SEGMENT_FILE_SIZE = sSTables.size
  private val emptyBuffer = Array.fill(SEGMENT_FILE_SIZE)(BUFFER_IS_EMPTY)
  private val keyBuffer = new Array[String](SEGMENT_FILE_SIZE)

  /**
    * 読み込みを行い, 次の値が存在するかどうかを確認する.
    * @return 全てのセグメントファイルがEOFに達していれば true
    */
  override def hasNext: Boolean = {
    fulfillBuffer()
    !emptyBuffer.forall(_ == REACHED_EOF)
  }

  private def readNextOf(i: Int): Unit = {
    if (emptyBuffer(i) == BUFFER_IS_EMPTY) {
      if (sSTables(i).hasNext) {
        keyBuffer(i) = sSTables(i).readKey()
        emptyBuffer(i) = BUFFER_IS_FILLED
      } else {
        emptyBuffer(i) = REACHED_EOF
      }
    }
  }

  private def fulfillBuffer(): Unit =
    (0 until SEGMENT_FILE_SIZE) foreach readNextOf

  private def bufferFilledMinKey(): String =
    (0 until SEGMENT_FILE_SIZE)
      .filter(emptyBuffer(_) == BUFFER_IS_FILLED)
      .map(keyBuffer(_))
      .min

  /**
    * @return 現在バッファに読み込まれているキーの中で最小のキーの中で最新の値を返す.
    */
  override def next(): (String, Value) = {
    val minKey = bufferFilledMinKey()

    @tailrec
    def loop(i: Int): Value =
      if (i < SEGMENT_FILE_SIZE) {
        if (emptyBuffer(i) == BUFFER_IS_FILLED && keyBuffer(i) == minKey) {
          val latest = sSTables(i).readValue()
          emptyBuffer(i) = BUFFER_IS_EMPTY
          loopAfterLatest(i + 1, latest)
        } else {
          loop(i + 1)
        }
      } else {
        Value.Deleted // not reach this code.
      }

    @tailrec
    def loopAfterLatest(i: Int, latest: Value): Value =
      if (i < SEGMENT_FILE_SIZE) {
        if (emptyBuffer(i) == BUFFER_IS_FILLED && keyBuffer(i) == minKey) {
          sSTables(i).skipValue()
          emptyBuffer(i) = BUFFER_IS_EMPTY
        }
        loopAfterLatest(i + 1, latest)
      } else latest

    (minKey, loop(0))
  }

}

object SSTableMergeIterator {

  def apply(sSTables: Seq[SSTable]) =
    new SSTableMergeIterator(
      sSTables
        .sortBy(_.sequenceNo)
        .map(_.newReader())
        .toIndexedSeq)

}
