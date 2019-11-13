package kvs

import java.io.RandomAccessFile
import java.nio.charset.Charset
import java.util.Scanner

import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.implicits._

class SimpleKvs(raf: RandomAccessFile) {

  private val charset = Charset.forName("ascii")

  private def format(key: String, value: String): String =
    s"$key,$value\n"

  def set(key: String, value: String): Unit = {
    raf.seek(raf.length())
    raf.writeBytes(format(key,value))
  }

  def get(key: String): Option[String] = {
    @scala.annotation.tailrec
    def seek(befPos: Long): Option[String] =
      if (befPos > 0) {
        var pos = befPos
        while (pos >= 0 && raf.read() != '\n') {
          pos -= 1
          if (pos >= 0) raf.seek(pos)
        }

        raf.seek(pos+1)
        val line = raf.readLine().split(",")

        if (line(0) == key) Some(line(1))
        else seek(pos - 1)
      } else None

    val pos = raf.length() - 1
    seek(pos)
  }

}

object SimpleKvs extends IOApp {

  private val databaseFileName = "data/simplekvs/database.txt"

  def simpleKvs[F[_]: Sync](databaseFileName: String): F[SimpleKvs] =
    for {
      file <- Sync[F].delay(new java.io.File(databaseFileName))
      _ <- if (!file.exists()) {
        Sync[F].delay(file.createNewFile()) >>= { created =>
          if (created) Sync[F].pure()
          else Sync[F].raiseError[Unit](new Throwable(s"can not initialize db file. ${file.getAbsolutePath}"))
        }
      } else Sync[F].unit
      raf <- Sync[F].delay(new java.io.RandomAccessFile(file, "rw"))
    } yield new SimpleKvs(raf)

  def scanner[F[_]: Sync]: F[Scanner] = Sync[F].delay(new Scanner(System.in))

  def interpreter[F[_]: Sync](sc: Scanner, kvs: SimpleKvs): F[Unit] =
    for {
      lineR <- Sync[F].delay(sc.nextLine().trim.split("\\s+")).attempt
      _ <- lineR match {
        case Right(line) =>
          line(0) match {
            case "set" => Sync[F].delay(kvs.set(line(1), line(2))) >> interpreter(sc, kvs)
            case "get" => Sync[F].delay(println(kvs.get(line(1)))) >> interpreter(sc, kvs)
            case other => Sync[F].raiseError[Unit](new Throwable(s"can not supported this command: $other"))
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