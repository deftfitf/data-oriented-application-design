package kvs.lsm.sstable

import java.io.RandomAccessFile

import kvs.lsm.sstable.SegmentFile._

private[sstable] case class SegmentFile private[sstable] (sequenceNo: Int) {

  private def filePath: String =
    s"$BASE_PATH/segment_file_$sequenceNo.txt"

  @throws[java.io.FileNotFoundException]
  private def raf(mode: String): RandomAccessFile = {
    val file = new java.io.File(filePath)
    new RandomAccessFile(file, mode)
  }

  @throws[java.io.FileNotFoundException]
  def newReadOnlyRaf(): RandomAccessFile = raf("r")

  @throws[java.io.FileNotFoundException]
  def newReadWriteRaf(): RandomAccessFile = raf("rw")

  @throws[IllegalStateException]
  def createSegmentFile(): Boolean = {
    val file = new java.io.File(filePath)
    if (file.exists())
      throw new IllegalStateException("already exist segment file")
    file.createNewFile()
  }

}

object SegmentFile {

  final val KEY_HEADER_SIZE = Integer.BYTES
  final val VALUE_HEADER_SIZE = Integer.BYTES
  final val HEADER_SIZE = Integer.BYTES * 2
  final val MIN_DATA_SIZE = HEADER_SIZE + 2
  final val TOMBSTONE = Integer.MIN_VALUE

  private val BASE_PATH: String = "data/simplekvs/lsm"

}
