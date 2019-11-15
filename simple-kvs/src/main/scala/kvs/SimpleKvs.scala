package kvs

import java.io.RandomAccessFile
import java.util.Scanner

import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.implicits._
import scala.collection.mutable

class SimpleKvs(raf: RandomAccessFile, keyIndex: mutable.Map[String, Long]) {

  private final val KEY_HEADER_SIZE = Integer.BYTES
  private final val VALUE_HEADER_SIZE = Integer.BYTES
  private final val HEADER_SIZE = Integer.BYTES * 2
  private final val MIN_DATA_SIZE = HEADER_SIZE + 2
  private final val tombstone = Integer.MIN_VALUE

  def set(key: String, value: String): Unit = {
    raf.seek(raf.length())
    raf.writeBytes(value)
    raf.writeInt(value.length)
    raf.writeBytes(key)
    raf.writeInt(key.length)
    keyIndex.update(key, raf.getFilePointer)
  }

  @scala.annotation.tailrec
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
      if (valueLen == tombstone) {
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

  def get(key: String): Option[String] =
    keyIndex.get(key) match {
      case Some(pos) => seek(key.getBytes(), pos)
      case None      => seek(key.getBytes(), raf.length())
    }

  def del(key: String): Unit =
    get(key) foreach { _ =>
      raf.seek(raf.length())
      raf.writeInt(tombstone)
      raf.writeBytes(key)
      raf.writeInt(key.length)
      keyIndex.update(key, raf.getFilePointer)
    }

}

object SimpleKvs extends IOApp {

  private val databaseFileName = "data/simplekvs/database.txt"

  def apply(databaseFileName: String): SimpleKvs = {
    val file = new java.io.File(databaseFileName)
    if (!file.exists() && !file.createNewFile())
      throw new Throwable(
        s"can not initialize db file. ${file.getAbsolutePath}")

    val raf = new java.io.RandomAccessFile(file, "rw")
    new SimpleKvs(raf, mutable.Map.empty)
  }

  def simpleKvs[F[_]: Sync](databaseFileName: String): F[SimpleKvs] =
    Sync[F].delay(apply(databaseFileName))

  def scanner[F[_]: Sync]: F[Scanner] = Sync[F].delay(new Scanner(System.in))

  def interpreter[F[_]: Sync](sc: Scanner, kvs: SimpleKvs): F[Unit] =
    for {
      lineR <- Sync[F].delay(sc.nextLine().trim.split("\\s+")).attempt
      _ <- lineR match {
        case Right(line) =>
          line(0) match {
            case "set" =>
              Sync[F].delay(kvs.set(line(1), line(2))) >> interpreter(sc, kvs)
            case "get" =>
              Sync[F].delay(println(kvs.get(line(1)))) >> interpreter(sc, kvs)
            case other =>
              Sync[F].raiseError[Unit](
                new Throwable(s"can not supported this command: $other"))
          }
        case Left(e) => Sync[F].raiseError[Unit](e)
      }
    } yield ()

  override def run(args: List[String]): IO[ExitCode] =
    for {
      kvs <- simpleKvs[IO](databaseFileName)
      sc <- scanner[IO]
      _ <- interpreter[IO](sc, kvs)
    } yield ExitCode.Success

}
