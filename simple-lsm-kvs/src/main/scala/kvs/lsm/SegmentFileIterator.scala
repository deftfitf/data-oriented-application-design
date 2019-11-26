package kvs.lsm

import java.io.RandomAccessFile

/**
 * @param raf ReadOnly Modeで開いたRandomAccessFile
 */
class SegmentFileIterator(raf: RandomAccessFile) {

  private val RAF_LENGTH = raf.length()

  def hasNext: Boolean = raf.getFilePointer < RAF_LENGTH

  /**
   * hasNext == true である場合に, 次のキーを取り出すことができる.
   * @return
   */
  def nextKey: String = {
    val keyLen = raf.readInt()
    val currentKey = new Array[Byte](keyLen)
    raf.readFully(currentKey)
    new String(currentKey)
  }

  /**
   * nextKeyの呼び出しの後に呼び出す事で値をスキップする.
   */
  def skipValue: Unit = {
    val valueLen = raf.readInt()
    if (valueLen != SSTable.TOMBSTONE) {
      raf.skipBytes(valueLen)
    }
  }

  /**
   * nextKeyの呼び出しの後に呼び出す事で値を取り出す.
   * @return 値が存在する場合その値を返し, 削除されている場合にはNoneを返す.
   */
  def nextValue: Option[String] = {
    val valueLen = raf.readInt()
    if (valueLen != SSTable.TOMBSTONE) {
      val nextValue = new Array[Byte](valueLen)
      raf.readFully(nextValue)
      Some(new String(nextValue))
    } else {
      None
    }
  }

}

object SegmentFileIterator {

  def

}
