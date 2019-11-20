package kvs.lsm

import akka.actor.typed._
import akka.actor.typed.scaladsl.{Behaviors, Routers}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext

sealed trait Log

case class MemTable(index: mutable.TreeMap[String, String]) extends Log {
  def get(key: String): Option[String] = index get key
}

object MemTable {
  def empty: MemTable =
    MemTable(mutable.TreeMap[String,String]()(Ordering.String))
}

case class SSTable(sequenceNo: Long, routerRef: ActorRef[SSTable.Get]) extends Log

object SSTable {
  final case class Get(key: String, replyTo: ActorRef[/* SSTableのReaderが受ける型 */])

  final case class SparseKeyIndex(
      keyIndexedSeq: IndexedSeq[String],
      keyIndexPosition: IndexedSeq[Long]) {

    def positionRangeOf(key: String): (Long, Long) = ???

  }

  def pool(sequenceNo: Int, sparseKeyIndex: SparseKeyIndex, size: Int): Behavior[Get] =
    Routers.pool(poolSize = size)(
      Behaviors.supervise(
        Behaviors.supervise(worker(sequenceNo, sparseKeyIndex))
          .onFailure[java.io.IOException](SupervisorStrategy.restart))
        .onFailure[java.io.IOError](SupervisorStrategy.restart))

  private def worker(sequenceNo: Int, sparseKeyIndex: SparseKeyIndex): Behavior[Get] =
    Behaviors.setup { context =>
      val raf = new java.io.RandomAccessFile("", "r")

      Behaviors.receiveMessage {
        case Get(key, replyTo) =>
          val (start, end) = sparseKeyIndex.positionRangeOf(key)
          val foundValue: String = ???
          replyTo ! /* SSTableのReaderが受ける型 */(foundValue)
          ???
      }
      .receiveSignal {
        case (_, signal) if signal == PreRestart || signal == PostStop =>
          raf.close()
          Behaviors.same
      }
    }

}

object LSMTreeClient {
  sealed trait Command
  final case class Got(value: Option[String]) extends Command
}

object LSMTree {
  sealed trait Command
  final case class Get(key: String, replyTo: ActorRef[LSMTreeClient.Command]) extends Command
  final case class Set() extends Command
  final case class Compacted(cmd: SSTableCompactionProcess.Compacted) extends Command
  final case class Merged(cmd: SSTableMergeProcess.Merged) extends Command

  private case class LogKey(underlying: Int)

  def apply(): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      implicit val ec: ExecutionContext =
        context.system.dispatchers.lookup(DispatcherSelector.)
      val memTable = MemTable.empty
      val lockedLogs: ListMap[LogKey, Log] = ListMap()

      def active(
          memTable: MemTable,
          lockedLogs: ListMap[LogKey, Log]): Behavior[Command] =
        Behaviors.receiveMessage {
          case Get(key, replyTo) =>
            memTable get key match {
              case Some(value) =>
                replyTo ! LSMTreeClient.Got(Some(value))
                Behaviors.same
              case None =>
                read(lockedLogs, replyTo)
            }
            Behaviors.same
          case _: Set => Behaviors.same
          case _: Compacted => Behaviors.same
          case _: Merged => Behaviors.same
        }

      active(memTable, lockedLogs)
    }

  private def read(
      lockedLogs: ListMap[LogKey, Log],
      replyTo: ActorRef[LSMTreeClient.Command]): Behavior[Unit] = ???

}

object SSTableCompactionProcess {
  sealed trait Command
  final case class Compact() extends Command
  final case class Compacted() extends Command

}

object SSTableMergeProcess {
  sealed trait Command
  final case class Merge() extends Command
  final case class Merged()
}