package kvs.lsm.sstable

import java.io.RandomAccessFile

import kvs.lsm.sstable.SSTable.Value

private[sstable] trait SegmentFileReadable {

  protected val readOnlyRaf: RandomAccessFile
  protected val RAF_LENGTH = readOnlyRaf.length()

  final def hasNext: Boolean = readOnlyRaf.getFilePointer < RAF_LENGTH

  @throws[java.io.IOException]
  def currentPointer(): Long =
    readOnlyRaf.getFilePointer

  @throws[java.io.IOException]
  def readKey(): String = new String(readKeyBytes())

  @throws[java.io.IOException]
  def readKeyBytes(): Array[Byte] = {
    val keyLen = readOnlyRaf.readInt()
    val currentKey = new Array[Byte](keyLen)
    readOnlyRaf.readFully(currentKey)
    currentKey
  }

  @throws[java.io.IOException]
  def skipKey(): Unit = {
    val keyLen = readOnlyRaf.readInt()
    val _ = readOnlyRaf.skipBytes(keyLen)
  }

  @throws[java.io.IOException]
  def readValue(): Value = {
    val valueLen = readOnlyRaf.readInt()
    if (valueLen != SegmentFile.TOMBSTONE) {
      val value = new Array[Byte](valueLen)
      readOnlyRaf.readFully(value)
      Value.Exist(new String(value))
    } else Value.Deleted
  }

  @throws[java.io.IOException]
  def skipValue(): Unit = {
    val valueLen = readOnlyRaf.readInt()
    if (valueLen != SegmentFile.TOMBSTONE) {
      val _ = readOnlyRaf.skipBytes(valueLen)
    }
  }

  @throws[java.io.IOException]
  def close(): Unit =
    readOnlyRaf.close()

}
