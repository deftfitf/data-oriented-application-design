package kvs

import java.io.RandomAccessFile

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class SimpleKvs(raf: RandomAccessFile, keyIndex: mutable.Map[String, Long]) {

  private final val KEY_HEADER_SIZE = Integer.BYTES
  private final val VALUE_HEADER_SIZE = Integer.BYTES
  private final val HEADER_SIZE = Integer.BYTES * 2
  private final val MIN_DATA_SIZE = HEADER_SIZE + 2
  private final val TOMBSTONE = Integer.MIN_VALUE

  def set(key: String, value: String): Try[Unit] = {
    val befPos = raf.length()
    try {
      raf.seek(raf.length())
      raf.writeBytes(value)
      raf.writeInt(value.length)
      raf.writeBytes(key)
      raf.writeInt(key.length)
    } catch {
      case e: java.io.IOException =>
        raf.setLength(befPos)
        return Failure(e)
    }
    keyIndex.update(key, raf.getFilePointer)
    Success(())
  }

  @scala.annotation.tailrec
  @throws[java.io.IOException]
  private def seek(key: Array[Byte], befPos: Long): Option[String] =
    if (befPos >= MIN_DATA_SIZE) {
      var pos = befPos
      raf.seek(pos)

      pos -= KEY_HEADER_SIZE
      raf.seek(pos)
      val keyLen = raf.readInt()
      pos -= keyLen
      raf.seek(pos)
      val currentKey = new Array[Byte](keyLen)
      raf.readFully(currentKey)

      pos -= VALUE_HEADER_SIZE
      raf.seek(pos)
      val valueLen = raf.readInt()
      if (valueLen == TOMBSTONE) {
        return None
      }
      pos -= valueLen

      if (currentKey.sameElements(key)) {
        val value = new Array[Byte](valueLen)
        raf.seek(pos)
        raf.readFully(value)
        Some(new String(value))
      } else {
        seek(key, pos)
      }
    } else None

  def get(key: String): Option[String] = {
    val pos = keyIndex.getOrElse(key, raf.length())
    try {
      seek(key.getBytes, pos)
    } catch {
      case _: java.io.IOException => None
    }
  }

  def del(key: String): Try[Unit] =
    get(key) match {
      case Some(_) =>
        val befPos = raf.length()
        try {
          raf.seek(raf.length())
          raf.writeInt(TOMBSTONE)
          raf.writeBytes(key)
          raf.writeInt(key.length)
        } catch {
          case e: java.io.IOException =>
            raf.setLength(befPos)
            return Failure(e)
        }
        keyIndex.update(key, raf.getFilePointer)
        Success(())

      case None =>
        Success(())
    }

}

object SimpleKvs {

  def apply(databaseFileName: String): SimpleKvs = {
    val file = new java.io.File(databaseFileName)
    if (!file.exists() && !file.createNewFile())
      throw new Throwable(
        s"can not initialize db file. ${file.getAbsolutePath}")

    val raf = new java.io.RandomAccessFile(file, "rw")
    new SimpleKvs(raf, mutable.Map.empty)
  }

}
