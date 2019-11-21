package kvs.lsm

import java.io.{File, FileInputStream, FileReader, InputStreamReader}

import akka.actor.typed._
import akka.actor.typed.scaladsl.{Behaviors, Routers}
import kvs.lsm.SSTable.SparseKeyIndex

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import kvs.lsm.LSMTree.Response.Got

import scala.io.Source

sealed trait Log

case class MemTable(index: mutable.TreeMap[String, String], maxSize: Int = 10000) extends Log {
  def isOverMaxSize: Boolean = index.size >= maxSize
  def get(key: String): Option[String] = index get key
  def set(key: String, value: String): Unit = index.update(key, value)
}

object MemTable {
  def empty: MemTable =
    MemTable(mutable.TreeMap[String,String]()(Ordering.String))
}

case class SSTable(routerRef: ActorRef[SSTable.Get]) extends Log

object SSTable {

  final val TOMBSTONE = Integer.MIN_VALUE

  final case class Get(key: String, replyTo: ActorRef[Got])

  sealed trait Got
  final case class Found(value: String) extends Got
  final case object NotFound extends Got
  final case object Deleted extends Got

  final case class SparseKeyIndex(keyIndex: IndexedSeq[String], positionIndex: IndexedSeq[Long]) {

    def positionOrRangeOf(key: String): Either[(Long, Option[Long]), Long] = {
      def binarySearch(l: Int, r: Int): Int =
        if (r - l > 1) {
          val center = l + (r - l) / 2
          if (keyIndex(center) > key) binarySearch(l, center)
          else binarySearch(center, r)
        } else l

      val leftIdx = binarySearch(-1, keyIndex.size)
      if (keyIndex(leftIdx) == key)
        Right(positionIndex(leftIdx))
      else if (leftIdx < keyIndex.size - 1)
        Left((positionIndex(leftIdx), Some(positionIndex(leftIdx+1))))
      else
        Left((positionIndex(leftIdx), None))
    }

  }

  def segmentFilePath(sequenceNo: Int): String =
    s"data/simplekvs/lsm/segment_file_$sequenceNo.txt"

  def pool(sequenceNo: Int, sparseKeyIndex: SparseKeyIndex, size: Int = 3): Behavior[Get] =
    Routers.pool(poolSize = size)(
      Behaviors.supervise(
        Behaviors.supervise(worker(sequenceNo, sparseKeyIndex))
          .onFailure[java.io.IOException](SupervisorStrategy.restart))
        .onFailure[java.io.IOError](SupervisorStrategy.restart))

  private def worker(sequenceNo: Int, sparseKeyIndex: SparseKeyIndex): Behavior[Get] =
    Behaviors.setup { _ =>
      val segmentFileName = SSTable.segmentFilePath(sequenceNo)
      val file = new java.io.File(segmentFileName)
      val raf = new java.io.RandomAccessFile(file, "r")

      Behaviors.receiveMessage[Get] {
        case Get(key, replyTo) =>
          val got = sparseKeyIndex.positionOrRangeOf(key) match {
            case Right(position) =>
              raf.seek(position)
              val keyLen = raf.readInt()
              raf.skipBytes(keyLen)
              val valueLen = raf.readInt()
              if (valueLen == TOMBSTONE) {
                replyTo ! Deleted
                return Behaviors.same
              }
              val value = new Array[Byte](keyLen)
              raf.readFully(value)
              Found(new String(value))
            case Left((left, Some(right))) => rangeSearch(raf, key.getBytes, left, right)
            case Left((left, None)) => rangeSearch(raf, key.getBytes, left, raf.length())

          }
          replyTo ! got
          Behaviors.same
      }
      .receiveSignal {
        case (_, signal) if signal == PreRestart || signal == PostStop =>
          raf.close()
          Behaviors.same
      }
    }

  private def rangeSearch(raf: java.io.RandomAccessFile, keyBytes: Array[Byte], start: Long, end: Long): Got = {
    def recursive(): Got = {
      if (raf.getFilePointer < end) {
        val keyLen = raf.readInt()
        val currentKey = new Array[Byte](keyLen)
        raf.readFully(currentKey)
        val valueLen = raf.readInt()
        if (currentKey.sameElements(keyBytes)) {
          if (valueLen != TOMBSTONE) {
            val value = new Array[Byte](valueLen)
            raf.readFully(value)
            Found(new String(value))
          } else Deleted
        } else {
          raf.skipBytes(valueLen)
          recursive()
        }
      } else NotFound
    }
    raf.seek(start)
    recursive()
  }

}

object LSMTree {

  sealed trait Command
  object Command {
    sealed trait Request extends Command
    object Request {
      final case class Get(key: String, replyTo: ActorRef[Response]) extends Request
      final case class Set(key: String, value: String, replyTo: ActorRef[Response]) extends Request
      final case class Del(key: String, replyTo: ActorRef[Response]) extends Request
    }
    final case class Applied(res: SSTableFactory.Applied) extends Command
    final case class Merged(res: MergeProcess.Merged) extends Command
  }

  sealed trait Response
  object Response {
    final case class Got(value: Option[String]) extends Response
    final case object Set extends Response
    final case object Deleted extends Response
  }

  def apply(): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      implicit val ec: ExecutionContext = ???
      val statistics = initializeStatistics()
      val memTable = MemTable.empty
      val lockedLogs = initializeLockedLogs(statistics)
      val sstableFactory = context.spawnAnonymous(SSTableFactory())
      val mergeProcess: ActorRef[MergeProcess.Merge] = ???

      def active(
          sequenceNo: Int,
          memTable: MemTable,
          lockedLogs: SortedMap[Int, Log],
          deleted: Set[String]): Behavior[Command] =
        Behaviors.receiveMessage[Command] {
          case Command.Request.Get(key, replyTo) =>
            memTable get key match {
              case Some(value) =>
                if (!deleted.contains(key)) {
                  replyTo ! Got(Some(value))
                } else {
                  replyTo ! Got(None)
                }
                Behaviors.same
              case None =>
                read(key, lockedLogs)(ec, context.system.scheduler)
                  .map {
                    case SSTable.NotFound | SSTable.Deleted => Got(None)
                    case SSTable.Found(value) => Got(Some(value))
                  }
                  .foreach(replyTo ! _)
            }
            Behaviors.same

          case Command.Request.Set(key, value, replyTo) =>
            memTable.set(key, value)
            replyTo ! Response.Set

            if (memTable.isOverMaxSize) {
              sstableFactory ! SSTableFactory.Apply(sequenceNo, memTable, deleted, context.self)

              val newMemTable = MemTable.empty
              val updatedLockedLogs = lockedLogs.updated(sequenceNo, memTable)
              active(sequenceNo+1, newMemTable, updatedLockedLogs, deleted - key)
            } else {
              active(sequenceNo, memTable, lockedLogs, deleted - key)
            }

          case Command.Request.Del(key, replyTo) =>
            replyTo ! Response.Deleted
            active(sequenceNo, memTable, lockedLogs, deleted + key)

          case Command.Applied(res) =>
            val updatedLockedLogs = lockedLogs.updated(res.sequenceNo, res.sstable)
            mergeProcess ! MergeProcess.Merge(updatedLockedLogs)
            active(sequenceNo, memTable, updatedLockedLogs, deleted)

          case Command.Merged(res) =>
            val mergedLockedLogs =
              res.mergedSegments.foldLeft(lockedLogs) { (logs, mergedSegment) =>
                val maxSequenceNo = mergedSegment._1.max
                mergedSegment._1.foldLeft(logs)(_ removed _)
                  .updated(maxSequenceNo, mergedSegment._2)
              }

            active(sequenceNo, memTable, mergedLockedLogs, deleted)

        }

      active(0, memTable, lockedLogs, Set.empty)
    }

  implicit val timeout: Timeout = 3.seconds

  private def read(key: String, lockedLogs: SortedMap[Int, Log])(implicit ec: ExecutionContext, scheduler: Scheduler): Future[SSTable.Got] =
    lockedLogs.values.foldLeft(Future.successful(SSTable.NotFound): Future[SSTable.Got]) { (fValue, log) =>
      for {
        value <- fValue
        res <- value match {
          case SSTable.NotFound =>
            log match {
              case memTable: MemTable => Future.successful(LSMTree.Response.Got(memTable get key))
              case sstable: SSTable => sstable.routerRef.ask(SSTable.Get(key, _) _)
            }
          case _ => value
        }
      } yield res
    }

  private def initializeStatistics(): Statistics = {
    val file = new File(statisticsFile)
    val reader = Source.fromFile(file).bufferedReader()
    try {
      val statistics = reader.readLine().split("\\s+")
      Statistics(
        lastSequenceNo = statistics(0).toInt,
        activeSequenceNos = statistics(1).split(",").map(_.toInt).toList)
    } finally {
      reader.close()
    }
  }

//  private def initializeLockedLogs(statistics: Statistics): SortedMap[Int, Log] = {
//    val lockedLogs = TreeMap()(Ordering.Int.reverse)
//    if (statistics.activeSequenceNos.isEmpty) lockedLogs
//    else {
//      val sparseKeyIndex =
//        statistics.activeSequenceNos.map(SSTable.segmentFilePath)
//          .map(SSTableFactory.sparseIndexFrom)
//      statistics.activeSequenceNos.zip(sparseKeyIndex)
//        .foldLeft(lockedLogs)((log, next) =>
//          log.updated(next._1, SSTable(SSTable.pool(next._1, next._2))))
//    }
//
//  }

  private val statisticsFile: String =
    "data/simplekvs/lsm/statistics.txt"

}

case class Statistics(lastSequenceNo: Int, activeSequenceNos: List[Int])

object SSTableFactory {

  sealed trait Command
  final case class Apply(sequenceNo: Int, memTable: MemTable, deleted: Set[String], replyTo: ActorRef[LSMTree.Command.Applied]) extends Command
  final case class Shutdown(actorRef: ActorRef[SSTable.Get]) extends Command

  final case class Applied(sequenceNo: Int, sstable: SSTable)

  final case object SegmentFileInitializeError extends Throwable
  final case object SparseKeyIndexInitializeError extends Throwable

  def apply(): Behavior[Command] =
    Behaviors.supervise(behavior).onFailure[Throwable](
      SupervisorStrategy.restart.withLimit(maxNrOfRetries = 3, withinTimeRange = 3.seconds))

  private def behavior: Behavior[Command] =
    Behaviors.receive[Command] { (context, msg) =>
      msg match {
        case Apply(sequenceNo, memTable, deleted, replyTo) =>
          val segmentFileName = SSTable.segmentFilePath(sequenceNo)

          val sparseKeyIndex = initializeSSTableFile(segmentFileName, memTable, deleted)
          val sstableRef = context.spawnAnonymous(SSTable.pool(sequenceNo, sparseKeyIndex))
          replyTo ! LSMTree.Command.Applied(Applied(sequenceNo, SSTable(sstableRef)))

          Behaviors.same
        case Shutdown(actorRef) =>
          context.stop(actorRef)
          Behaviors.same
      }
    }

  final private val SPARSE_INDEX_PER = 100

  private def initializeSSTableFile(segmentFileName: String, memTable: MemTable, deleted: Set[String]): SparseKeyIndex = {
    val file = new java.io.File(segmentFileName)
    if (file.exists()) file.delete()
    if (!file.createNewFile()) throw SegmentFileInitializeError
    val raf = new java.io.RandomAccessFile(file, "rw")

    val keyIndex = mutable.ArrayBuffer[String]()
    val positionIndex = mutable.ArrayBuffer[Long]()

    raf.seek(0)
    memTable.index.iterator.zipWithIndex.foreach { case ((key, value), idx) =>
      if (idx % SPARSE_INDEX_PER == 0) {
        keyIndex.addOne(key)
        positionIndex.addOne(raf.getFilePointer)
      }
      raf.writeInt(key.length)
      raf.writeBytes(key)
      if (!deleted.contains(key)) {
        raf.writeInt(value.length)
        raf.writeBytes(value)
      } else {
        raf.writeInt(SSTable.TOMBSTONE)
      }
    }
    raf.setLength(raf.getFilePointer)
    raf.close()

    SparseKeyIndex(keyIndex.toIndexedSeq, positionIndex.toIndexedSeq)
  }

  def sparseIndexFrom(segmentFileName: String): SparseKeyIndex = {
    val file = new java.io.File(segmentFileName)
    if (file.exists()) file.delete()
    if (!file.createNewFile()) throw SparseKeyIndexInitializeError
    val raf = new java.io.RandomAccessFile(file, "r")

    val keyIndex = mutable.ArrayBuffer[String]()
    val positionIndex = mutable.ArrayBuffer[Long]()

    def pickUpKeys(): Unit = {
      def recursive(count: Int): Unit = {
        val pointer = raf.getFilePointer
        if (pointer < raf.length()) {
          val keyLen = raf.readInt()
          if (count % SPARSE_INDEX_PER == 0) {
            val currentKey = new Array[Byte](keyLen)
            raf.readFully(currentKey)
            keyIndex.addOne(new String(currentKey))
            positionIndex.addOne(pointer)
          } else {
            raf.skipBytes(keyLen)
          }
          val valueLen = raf.readInt()
          if (valueLen != SSTable.TOMBSTONE) {
            raf.skipBytes(valueLen)
          }
          recursive(count+1)
        } else ()
      }
      raf.seek(0)
      recursive(0)
    }
    pickUpKeys()

    SparseKeyIndex(keyIndex.toIndexedSeq, positionIndex.toIndexedSeq)
  }

}

object MergeProcess {
  final case class Merge(lockedLogs: SortedMap[Int, Log])
  final case class Merged(mergedSegments: List[(List[Int], SSTable)])
}