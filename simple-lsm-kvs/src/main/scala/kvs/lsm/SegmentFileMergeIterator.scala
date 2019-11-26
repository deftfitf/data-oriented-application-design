package kvs.lsm

import SegmentFileMergeIterator._

/**
 * @param segmentFiles 新しい順に並んだセグメントファイルのIterator
 */
class SegmentFileMergeIterator(segmentFiles: IndexedSeq[SegmentFileIterator]) extends Iterator[(String, Value)] {

  private val BUFFER_IS_EMPTY = 0
  private val BUFFER_IS_FILLED = 1
  private val REACHED_EOF = 2

  private val SEGMENT_FILE_SIZE = segmentFiles.size
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
      if (segmentFiles(i).hasNext) {
        keyBuffer(i) = segmentFiles(i).nextKey
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

    def loop(i: Int): Value =
      if (i < SEGMENT_FILE_SIZE) {
        if (emptyBuffer(i) == BUFFER_IS_FILLED && keyBuffer(i) == minKey) {
          val value = segmentFiles(i).nextValue
          emptyBuffer(i) = BUFFER_IS_EMPTY
          val latest = value match {
            case Some(value) => Exist(value)
            case None => Deleted
          }
          loopAfterLatest(i+1, latest)
        } else {
          loop(i+1)
        }
      } else {
        Deleted // not reach this code.
      }

    def loopAfterLatest(i: Int, latest: Value): Value =
      if (i < SEGMENT_FILE_SIZE) {
        if (emptyBuffer(i) == BUFFER_IS_FILLED && keyBuffer(i) == minKey) {
          segmentFiles(i).skipValue
          emptyBuffer(i) = BUFFER_IS_EMPTY
        }
        loopAfterLatest(i+1, latest)
      } else latest

    (minKey, loop(0))
  }

}

object SegmentFileMergeIterator {

  sealed trait Value
  case class Exist(value: String) extends Value
  case object Deleted extends Value

}