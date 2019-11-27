package kvs.lsm.behavior

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector, Scheduler}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import kvs.lsm.Log
import kvs.lsm.Log.{MemTable, SSTableRef}
import kvs.lsm.behavior.LSMTreeBehavior.Response.{Got, UnInitialized}
import kvs.lsm.sstable.{SSTable, SSTableFactory}
import kvs.lsm.statistics.Statistics

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object LSMTreeBehavior {

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

    final case class Applied(res: SSTableFactoryBehavior.Applied)
        extends Command
    final case class Merged(res: SSTableMergeBehavior.Merged) extends Command

  }

  sealed trait Response
  object Response {
    final case class Got(value: SSTable.Got) extends Response
    final case object Deleted extends Response
    final case object Set extends Response
    final case object UnInitialized extends Response
  }

  sealed trait State
  final case class Initializing(lockedLogs: SortedMap[Int, Log])
  final case class Initialized(
      context: ActorContext[Command],
      sequenceNo: Int,
      memTable: MemTable,
      lockedLogs: SortedMap[Int, Log],
      sSTableFactoryBehavior: ActorRef[SSTableFactoryBehavior.Command],
      sSTableMergeBehavior: ActorRef[SSTableMergeBehavior.Merge])

  implicit val timeout: Timeout = 3.seconds

  def start(sSTableFactory: SSTableFactory,
            readerPoolSize: Int): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      implicit val ec: ExecutionContext =
        context.system.dispatchers.lookup(DispatcherSelector.blocking())
      val sSTableFactoryBehavior = context.spawnAnonymous(
        SSTableFactoryBehavior(sSTableFactory, readerPoolSize))
      val sSTableMergeBehavior = context.spawnAnonymous(
        SSTableMergeBehavior(sSTableFactory, readerPoolSize))

      val statistics = Statistics.initialize()
      val memTable: MemTable = statistics.recoveryMemTable()
      val lockedLogs = TreeMap[Int, Log]()(Ordering.Int.reverse)

      val adapter = context.messageAdapter(Command.Applied)
      statistics.activeSequenceNos.foreach {
        sSTableFactoryBehavior ! SSTableFactoryBehavior.Initialize(_, adapter)
      }

      def initialize(count: Int, state: Initializing): Behavior[Command] =
        if (count < statistics.activeSequenceNos.size) {
          Behaviors.receiveMessagePartial {
            case Command.Applied(res) =>
              initialize(count + 1,
                         state.copy(lockedLogs =
                           lockedLogs.updated(res.sequenceNo, res.sSTableRef)))
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
              sSTableFactoryBehavior = sSTableFactoryBehavior,
              sSTableMergeBehavior = sSTableMergeBehavior
            ))
        }

      initialize(0, Initializing(lockedLogs))
    }

  def active(state: Initialized)(
      implicit ec: ExecutionContext): Behavior[Command] =
    Behaviors.receiveMessage[Command] {
      case Command.Request.Get(key, replyTo) =>
        state.memTable get key match {
          case SSTable.Got.NotFound =>
            read(key, state.lockedLogs)(ec, state.context.system.scheduler)
              .map(Got)
              .foreach(replyTo ! _)
          case got =>
            replyTo ! Got(got)
            Behaviors.same
        }
        Behaviors.same

      case Command.Request.Set(key, value, replyTo) =>
        state.memTable.set(key, value)
        replyTo ! Response.Set

        if (state.memTable.isOverMaxSize) {
          val adapter = state.context.messageAdapter(Command.Applied)
          state.sSTableFactoryBehavior ! SSTableFactoryBehavior.Apply(
            state.sequenceNo,
            state.memTable,
            adapter)

          val newMemTable = MemTable.empty
          val updatedLockedLogs =
            state.lockedLogs.updated(state.sequenceNo, state.memTable)

          active(
            state.copy(sequenceNo = state.sequenceNo + 2,
                       memTable = newMemTable,
                       lockedLogs = updatedLockedLogs))
        } else {
          Behaviors.same
        }

      case Command.Request.Del(key, replyTo) =>
        replyTo ! Response.Deleted
        state.memTable.del(key)
        Behaviors.same

      case Command.Applied(res) =>
        val updatedLockedLogs =
          state.lockedLogs.updated(res.sequenceNo, res.sSTableRef)
        val adapter = state.context.messageAdapter(Command.Merged)
        val mergeSegments = updatedLockedLogs
          .takeWhile(_._2.isInstanceOf[SSTable])
          .map[SSTable](_._2.asInstanceOf[SSTable])
          .toSeq

        if (mergeSegments.size >= 2) {
          state.sSTableMergeBehavior ! SSTableMergeBehavior.Merge(
            res.sequenceNo + 1,
            mergeSegments,
            adapter)
        }
        active(state.copy(lockedLogs = updatedLockedLogs))

      case Command.Merged(res) =>
        val mergedLockedLogs =
          res.removedSequenceNo
            .foldLeft(state.lockedLogs)(_ removed _)
            .updated(res.mergedSegment.sSTable.sequenceNo, res.mergedSegment)

        active(state.copy(lockedLogs = mergedLockedLogs))

    }

  private def read(key: String, lockedLogs: SortedMap[Int, Log])(
      implicit ec: ExecutionContext,
      scheduler: Scheduler): Future[SSTable.Got] =
    lockedLogs.values.foldLeft(
      Future.successful(SSTable.Got.NotFound): Future[SSTable.Got]) {
      (fValue, log) =>
        for {
          value <- fValue
          res <- value match {
            case SSTable.Got.NotFound =>
              log match {
                case memTable: MemTable =>
                  Future.successful(memTable get key)
                case sSTableRef: SSTableRef =>
                  sSTableRef.routerRef
                    .ask[SSTable.Got](SSTableBehavior.Get(key, _))
              }
            case _ => Future.successful(value)
          }
        } yield res
    }

}
