package kvs.lsm

import java.io.{File, FileWriter}

import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, Routers}
import akka.util.Timeout
import kvs.lsm.LSMTree.Response.{Got, UnInitialized}
import kvs.lsm.SSTable.SparseKeyIndex

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

object Client {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem(LSMTree.start(), "lsm-tree")
    sys.addShutdownHook {
      system.terminate()
    }

    def result: Behavior[LSMTree.Response] = {
      Behaviors.receiveMessage {
        case LSMTree.Response.UnInitialized =>
          println("uninitialized"); Behaviors.same
        case LSMTree.Response.Deleted => println("deleted"); Behaviors.same
        case LSMTree.Response.Got(v)  => println(s"got: $v"); Behaviors.same
        case LSMTree.Response.Set     => println("set"); Behaviors.same
      }
    }

    val r = system.systemActorOf(result, "r")

    val sc = new java.util.Scanner(System.in)
    while (sc.hasNextLine()) {
      val cmd = sc.nextLine().trim.split("\\s+")
      cmd(0).toLowerCase match {
        case "get" => system ! LSMTree.Command.Request.Get(cmd(1), r)
        case "set" => system ! LSMTree.Command.Request.Set(cmd(1), cmd(2), r)
        case "del" => system ! LSMTree.Command.Request.Del(cmd(1), r)
      }
    }
  }

}

sealed trait Log

case class MemTable(index: mutable.TreeMap[String, String],
                    maxSize: Int = 10000)
    extends Log {
  def isOverMaxSize: Boolean = index.size >= maxSize
  def get(key: String): Option[String] = index get key
  def set(key: String, value: String): Unit = index.update(key, value)
}

object MemTable {
  def empty: MemTable =
    MemTable(mutable.TreeMap[String, String]()(Ordering.String))
}

case class SSTable(sequenceNo: Int, routerRef: ActorRef[SSTable.Get])
    extends Log

object SSTable {

  final val TOMBSTONE = Integer.MIN_VALUE

  final case class Get(key: String, replyTo: ActorRef[Got])

  sealed trait Got
  final case class Found(value: String) extends Got
  final case object NotFound extends Got
  final case object Deleted extends Got

  final case class SparseKeyIndex(keyIndex: IndexedSeq[String],
                                  positionIndex: IndexedSeq[Long]) {

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
        Left((positionIndex(leftIdx), Some(positionIndex(leftIdx + 1))))
      else
        Left((positionIndex(leftIdx), None))
    }

  }

  def segmentFilePath(sequenceNo: Int): String =
    s"data/simplekvs/lsm/segment_file_$sequenceNo.txt"

  def pool(sequenceNo: Int,
           sparseKeyIndex: SparseKeyIndex,
           size: Int = 3): Behavior[Get] =
    Routers.pool(poolSize = size)(
      Behaviors
        .supervise(
          Behaviors
            .supervise(worker(sequenceNo, sparseKeyIndex))
            .onFailure[java.io.IOException](SupervisorStrategy.restart))
        .onFailure[java.io.IOError](SupervisorStrategy.restart))

  private def worker(sequenceNo: Int,
                     sparseKeyIndex: SparseKeyIndex): Behavior[Get] =
    Behaviors.setup { _ =>
      val segmentFileName = SSTable.segmentFilePath(sequenceNo)
      val file = new java.io.File(segmentFileName)
      val raf = new java.io.RandomAccessFile(file, "r")

      Behaviors
        .receiveMessage[Get] {
          case Get(key, replyTo) =>
            val got = sparseKeyIndex.positionOrRangeOf(key) match {
              case Right(position) =>
                raf.seek(position)
                val keyLen = raf.readInt()
                raf.skipBytes(keyLen)
                val valueLen = raf.readInt()
                if (valueLen == TOMBSTONE) Deleted
                else {
                  val value = new Array[Byte](keyLen)
                  raf.readFully(value)
                  Found(new String(value))
                }
              case Left((left, Some(right))) =>
                rangeSearch(raf, key.getBytes, left, right)
              case Left((left, None)) =>
                rangeSearch(raf, key.getBytes, left, raf.length())

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

  private def rangeSearch(raf: java.io.RandomAccessFile,
                          keyBytes: Array[Byte],
                          start: Long,
                          end: Long): Got = {
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
    sealed trait Request extends Command {
      val replyTo: ActorRef[Response]
    }
    object Request {
      final case class Get(key: String, replyTo: ActorRef[Response])
          extends Request
      final case class Set(key: String,
                           value: String,
                           replyTo: ActorRef[Response])
          extends Request
      final case class Del(key: String, replyTo: ActorRef[Response])
          extends Request
    }
    final case class Applied(res: SSTableFactory.Applied) extends Command
    //final case class Merged(res: MergeProcess.Merged) extends Command
  }

  sealed trait Response
  object Response {
    final case class Got(value: Option[String]) extends Response
    final case object Set extends Response
    final case object Deleted extends Response
    final case object UnInitialized extends Response
  }

  sealed trait State
  final case class Initializing(lockedLogs: SortedMap[Int, Log])
  final case class Initialized(
      context: ActorContext[Command],
      sequenceNo: Int,
      memTable: MemTable,
      lockedLogs: SortedMap[Int, Log],
      deleted: Set[String],
      sstableFactory: ActorRef[SSTableFactory.Command],
      /**mergeProcess: ActorRef[MergeProcess.Merge]**/ )

  def start(): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      implicit val ec: ExecutionContext =
        context.system.dispatchers.lookup(DispatcherSelector.blocking())
      val sstableFactory = context.spawnAnonymous(SSTableFactory())
      //val mergeProcess: ActorRef[MergeProcess.Merge] = ???

      val statistics = initializeStatistics()
      val sparseKeyIndexes = initializeSparseKeyIndexes(statistics)
      val memTable = MemTable.empty
      val deleted = Set.empty[String]
      val lockedLogs = TreeMap[Int, Log]()(Ordering.Int.reverse)

      val adapter = context.messageAdapter(Command.Applied)
      statistics.activeSequenceNos.zip(sparseKeyIndexes).foreach {
        case (seq, sparse) =>
          sstableFactory ! SSTableFactory.Initialize(seq, sparse, adapter)
      }

      def initilize(count: Int, state: Initializing): Behavior[Command] =
        if (count < statistics.activeSequenceNos.size) {
          Behaviors.receiveMessagePartial {
            case Command.Applied(res) =>
              initilize(
                count + 1,
                state.copy(
                  lockedLogs = lockedLogs.updated(res.sequenceNo, res.sstable)))
            case other: Command.Request =>
              other.replyTo ! UnInitialized
              Behaviors.same
          }
        } else {
          active(
            Initialized(
              context = context,
              sequenceNo = statistics.lastSequenceNo + 1,
              memTable = memTable,
              lockedLogs = state.lockedLogs,
              deleted = deleted,
              sstableFactory = sstableFactory,
              /**mergeProcess = mergeProcess**/
            ))
        }

      initilize(0, Initializing(lockedLogs))
    }

  def active(state: Initialized)(
      implicit ec: ExecutionContext): Behavior[Command] =
    Behaviors.receiveMessage[Command] {
      case Command.Request.Get(key, replyTo) =>
        state.memTable get key match {
          case Some(value) =>
            if (!state.deleted.contains(key)) {
              replyTo ! Got(Some(value))
            } else {
              replyTo ! Got(None)
            }
            Behaviors.same
          case None =>
            read(key, state.lockedLogs)(ec, state.context.system.scheduler)
              .map {
                case SSTable.NotFound | SSTable.Deleted => Got(None)
                case SSTable.Found(value)               => Got(Some(value))
              }
              .foreach(replyTo ! _)
        }
        Behaviors.same

      case Command.Request.Set(key, value, replyTo) =>
        state.memTable.set(key, value)
        replyTo ! Response.Set

        if (state.memTable.isOverMaxSize) {
          val adapter = state.context.messageAdapter(Command.Applied)
          state.sstableFactory ! SSTableFactory.Apply(state.sequenceNo,
                                                      state.memTable,
                                                      state.deleted,
                                                      adapter)

          val newMemTable = MemTable.empty
          val updatedLockedLogs =
            state.lockedLogs.updated(state.sequenceNo, state.memTable)
          active(
            state.copy(sequenceNo = state.sequenceNo + 2,
                       memTable = newMemTable,
                       lockedLogs = updatedLockedLogs,
                       deleted = state.deleted - key))
        } else {
          active(state.copy(deleted = state.deleted - key))
        }

      case Command.Request.Del(key, replyTo) =>
        replyTo ! Response.Deleted
        active(state.copy(deleted = state.deleted + key))

      case Command.Applied(res) =>
        val updatedLockedLogs =
          state.lockedLogs.updated(res.sequenceNo, res.sstable)
//        val adapter = state.context.messageAdapter(Command.Merged)
//        val mergeSegments = state.lockedLogs
//          .dropWhile(!_._2.isInstanceOf[SSTable])
//          .takeWhile(_._2.isInstanceOf[SSTable])
//          .map[SSTable](_._2.asInstanceOf[SSTable])
//          .map(_.sequenceNo).toList
//        if (mergeSegments.size >= 2) {
//          state.mergeProcess ! MergeProcess.Merge(mergeSegments, adapter)
//        }
        active(state.copy(lockedLogs = updatedLockedLogs))

//      case Command.Merged(res) =>
//        val mergedLockedLogs =
//          res.remove.foldLeft(state.lockedLogs)(_ removed _)
//              .updated(res.mergedSegment.sequenceNo, res.mergedSegment)
//
//        active(state.copy(lockedLogs = mergedLockedLogs))

    }

  implicit val timeout: Timeout = 3.seconds

  private def read(key: String, lockedLogs: SortedMap[Int, Log])(
      implicit ec: ExecutionContext,
      scheduler: Scheduler): Future[SSTable.Got] =
    lockedLogs.values.foldLeft(
      Future.successful(SSTable.NotFound): Future[SSTable.Got]) {
      (fValue, log) =>
        for {
          value <- fValue
          res <- value match {
            case SSTable.NotFound =>
              log match {
                case memTable: MemTable =>
                  Future.successful {
                    memTable get key match {
                      case Some(v) => SSTable.Found(v)
                      case None    => SSTable.NotFound
                    }
                  }
                case sstable: SSTable =>
                  sstable.routerRef.ask[SSTable.Got](SSTable.Get(key, _))
              }
            case _ => Future.successful(value)
          }
        } yield res
    }

  private def initializeStatistics(): Statistics = {
    val file = new File(statisticsFile)
    if (!file.exists()) {
      file.createNewFile()
      val w = new FileWriter(file)
      w.write("0")
      w.flush()
      w.close()
    }
    val reader = Source.fromFile(file).bufferedReader()

    try {
      val statistics = reader.readLine().split("\\s+")
      val lastSequenceNo = statistics(0).toInt
      val activeSequenceNos = if (lastSequenceNo > 0) statistics(1).split(",").map(_.toInt).toList else Nil
      Statistics(lastSequenceNo = lastSequenceNo,
                 activeSequenceNos = activeSequenceNos)
    } finally {
      reader.close()
    }
  }

  private def initializeSparseKeyIndexes(
      statistics: Statistics): Seq[SparseKeyIndex] =
    if (statistics.activeSequenceNos.isEmpty) Nil
    else
      statistics.activeSequenceNos
        .map(SSTable.segmentFilePath)
        .map(SSTableFactory.sparseIndexFrom)

  private val statisticsFile: String =
    "data/simplekvs/lsm/statistics.txt"

}

case class Statistics(lastSequenceNo: Int, activeSequenceNos: List[Int])

object SSTableFactory {

  sealed trait Command
  final case class Apply(sequenceNo: Int,
                         memTable: MemTable,
                         deleted: Set[String],
                         replyTo: ActorRef[Applied])
      extends Command
  final case class Initialize(sequenceNo: Int,
                              sparseKeyIndex: SparseKeyIndex,
                              replyTo: ActorRef[Applied])
      extends Command
  final case class Shutdown(actorRef: ActorRef[SSTable.Get]) extends Command

  final case class Applied(sequenceNo: Int, sstable: SSTable)

  final case object SegmentFileInitializeError extends Throwable
  final case object SparseKeyIndexInitializeError extends Throwable

  def apply(): Behavior[Command] =
    Behaviors
      .supervise(behavior)
      .onFailure[Throwable](SupervisorStrategy.restart
        .withLimit(maxNrOfRetries = 3, withinTimeRange = 3.seconds))

  private def behavior: Behavior[Command] =
    Behaviors.receive[Command] { (context, msg) =>
      msg match {
        case Initialize(sequenceNo, sparseKeyIndex, replyTo) =>
          val sstableRef =
            context.spawnAnonymous(SSTable.pool(sequenceNo, sparseKeyIndex))
          replyTo ! Applied(sequenceNo, SSTable(sequenceNo, sstableRef))
          Behaviors.same

        case Apply(sequenceNo, memTable, deleted, replyTo) =>
          val segmentFileName = SSTable.segmentFilePath(sequenceNo)
          val sparseKeyIndex =
            initializeSSTableFile(segmentFileName, memTable, deleted)
          val sstableRef =
            context.spawnAnonymous(SSTable.pool(sequenceNo, sparseKeyIndex))
          replyTo ! Applied(sequenceNo, SSTable(sequenceNo, sstableRef))
          Behaviors.same

        case Shutdown(actorRef) =>
          context.stop(actorRef)
          Behaviors.same
      }
    }

  final private val SPARSE_INDEX_PER = 100

  private def initializeSSTableFile(segmentFileName: String,
                                    memTable: MemTable,
                                    deleted: Set[String]): SparseKeyIndex = {
    val file = new java.io.File(segmentFileName)
    if (file.exists()) file.delete()
    if (!file.createNewFile()) throw SegmentFileInitializeError
    val raf = new java.io.RandomAccessFile(file, "rw")

    val keyIndex = mutable.ArrayBuffer[String]()
    val positionIndex = mutable.ArrayBuffer[Long]()

    raf.seek(0)
    memTable.index.iterator.zipWithIndex.foreach {
      case ((key, value), idx) =>
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
    if (!file.exists()) throw SparseKeyIndexInitializeError
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
          recursive(count + 1)
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
  final case class Merge(mergeSegments: List[Int], replyTo: ActorRef[Merged])
  final case class Merged(mergedSegment: SSTable, remove: List[Int])

  final case object MergeException extends Throwable

  def apply(): Behavior[Merge] =
    Behaviors.supervise(behavior)
      .onFailure[Throwable](SupervisorStrategy.restart)

  private def behavior: Behavior[Merge] =
    Behaviors.receiveMessage {
      case Merge(mergeSegments, replyTo) =>
        val merged = iterative2WayMerge(mergeSegments)
        replyTo ! Merged(merged, mergeSegments)
        Behaviors.same
    }

  private def readonlyRandomAccessFile(segmentNo: Int): RandomAccessFile = {
    val file = new java.io.File(SSTable.segmentFilePath(segmentNo))
    if (!file.exists()) throw MergeException
    new java.io.RandomAccessFile(file, "r")
  }

  private def createRandomAccessFile(segmentNo: Int): RandomAccessFile = {
    val file = new java.io.File(SSTable.segmentFilePath(segmentNo))
    if (file.exists()) throw MergeException
    file.createNewFile()
    new java.io.RandomAccessFile(file, "rw")
  }

  private def iterative2WayMerge(sstables: List[Int]): SSTable = {
    val base = readonlyRandomAccessFile()
    sstables.map(readonlyRandomAccessFile).reduce { (merged, next) =>

      merged
    }
    ???
  }

  private def twoWayMerge(baseRaf: RandomAccessFile, )

}
