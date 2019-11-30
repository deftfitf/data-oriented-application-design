package kvs.lsm.behavior

import akka.actor.typed._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.util.Timeout
import kvs.lsm.behavior.LSMTreeBehavior.Response.{Got, UnInitialized}
import kvs.lsm.sstable.Log.MemTable
import kvs.lsm.sstable.WriteAheadLog.{
  WriteAheadLogInitializeError,
  WriteAheadLogRecoveryError
}
import kvs.lsm.sstable.{Logs, SSTable, SSTableFactory, WriteAheadLog}
import kvs.lsm.statistics.Statistics
import kvs.lsm.statistics.Statistics.StatisticsInitializeError

import scala.concurrent.ExecutionContext
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
  final case class Initializing(logs: Logs)
  final case class Initialized(
      context: ActorContext[Command],
      nextSequenceNo: Int,
      memTable: MemTable,
      writeAheadLog: WriteAheadLog,
      logs: Logs,
      statistics: Statistics,
      sSTableFactoryBehavior: ActorRef[SSTableFactoryBehavior.Command],
      sSTableMergeBehavior: ActorRef[SSTableMergeBehavior.Merge])

  implicit val timeout: Timeout = 3.seconds

  def apply(sSTableFactory: SSTableFactory,
            readerPoolSize: Int,
            writeAheadLogPath: String,
            blockingIoDispatcher: DispatcherSelector): Behavior[Command] =
    Behaviors
      .supervise(
        Behaviors
          .supervise(
            Behaviors
              .supervise(Behaviors
                .supervise(start(sSTableFactory,
                                 readerPoolSize,
                                 writeAheadLogPath,
                                 blockingIoDispatcher))
                .onFailure[StatisticsInitializeError](SupervisorStrategy.stop))
              .onFailure[WriteAheadLogInitializeError](SupervisorStrategy.stop))
          .onFailure[WriteAheadLogRecoveryError](SupervisorStrategy.stop))
      .onFailure[Throwable](SupervisorStrategy.restart)

  def start(sSTableFactory: SSTableFactory,
            readerPoolSize: Int,
            writeAheadLogPath: String,
            blockingIoDispatcher: DispatcherSelector): Behavior[Command] =
    Behaviors.setup[Command] { context =>
      context.log.info("LSMTree initializing...")
      implicit val ec: ExecutionContext =
        context.system.dispatchers.lookup(blockingIoDispatcher)
      val sSTableFactoryBehavior = context.spawnAnonymous(
        SSTableFactoryBehavior(sSTableFactory, readerPoolSize),
        blockingIoDispatcher)
      val sSTableMergeBehavior = context.spawnAnonymous(
        SSTableMergeBehavior(sSTableFactory, readerPoolSize),
        blockingIoDispatcher)

      val statistics = Statistics.initialize()
      val writeAheadLog = WriteAheadLog.initialize(writeAheadLogPath)
      val memTable: MemTable = writeAheadLog.recovery()
      val logs = Logs.empty

      val adapter = context.messageAdapter(Command.Applied)
      statistics.activeSequenceNo.foreach {
        sSTableFactoryBehavior ! SSTableFactoryBehavior.Initialize(_, adapter)
      }

      def initialize(count: Int, state: Initializing): Behavior[Command] =
        if (count < statistics.activeSequenceNo.size) {
          Behaviors.receiveMessagePartial {
            case Command.Applied(res) =>
              initialize(
                count + 1,
                state.copy(logs = logs.updated(res.sequenceNo, res.sSTableRef)))
            case other: Command.Request =>
              other.replyTo ! UnInitialized
              Behaviors.same
          }
        } else {
          context.log.info("LSMTree initialized...")
          active(
            Initialized(
              context = context,
              nextSequenceNo = statistics.nextSequenceNo,
              memTable = memTable,
              writeAheadLog = writeAheadLog,
              logs = state.logs,
              statistics = statistics,
              sSTableFactoryBehavior = sSTableFactoryBehavior,
              sSTableMergeBehavior = sSTableMergeBehavior
            ))
        }

      initialize(0, Initializing(logs))
    }

  def active(state: Initialized)(
      implicit ec: ExecutionContext): Behavior[Command] =
    Behaviors
      .receiveMessage[Command] {
        case Command.Request.Get(key, replyTo) =>
          state.memTable get key match {
            case SSTable.Got.NotFound =>
              state.logs
                .read(key)(ec, timeout, state.context.system.scheduler)
                .map(Got)
                .foreach(replyTo ! _)
            case got =>
              replyTo ! Got(got)
              Behaviors.same
          }
          Behaviors.same

        case Command.Request.Set(key, value, replyTo) =>
          state.writeAheadLog.set(key, value)
          state.memTable.set(key, value)
          replyTo ! Response.Set

          if (state.memTable.isOverMaxSize) {
            val adapter = state.context.messageAdapter(Command.Applied)
            state.sSTableFactoryBehavior ! SSTableFactoryBehavior.Apply(
              state.nextSequenceNo,
              state.memTable,
              adapter)

            val newMemTable = MemTable.empty
            val updatedLogs =
              state.logs.updated(state.nextSequenceNo, state.memTable)
            state.writeAheadLog.clear()

            active(
              state.copy(nextSequenceNo = state.nextSequenceNo + 2,
                         memTable = newMemTable,
                         logs = updatedLogs))
          } else {
            Behaviors.same
          }

        case Command.Request.Del(key, replyTo) =>
          state.writeAheadLog.del(key)
          state.memTable.del(key)
          replyTo ! Response.Deleted
          Behaviors.same

        case Command.Applied(res) =>
          val updatedLogs =
            state.logs.updated(res.sequenceNo, res.sSTableRef)
          val adapter = state.context.messageAdapter(Command.Merged)

          state.statistics.updateStatistics(state.nextSequenceNo, updatedLogs)

          updatedLogs.mergeableSSTables
            .foreach {
              state.sSTableMergeBehavior ! SSTableMergeBehavior.Merge(
                res.sequenceNo + 1,
                _,
                adapter)
            }
          active(state.copy(logs = updatedLogs))

        case Command.Merged(res) =>
          val updatedLogs =
            state.logs.merged(res.removedSequenceNo, res.mergedSegment)

          state.statistics.updateStatistics(state.nextSequenceNo, updatedLogs)
          active(state.copy(logs = updatedLogs))

      }
      .receiveSignal {
        case (context, PostStop) =>
          context.log.info("LSMTree stopped.")
          state.writeAheadLog.close()
          state.logs.sSTableRefs.foreach { ref =>
            state.sSTableFactoryBehavior ! SSTableFactoryBehavior.Shutdown(
              ref.routerRef)
          }
          Behaviors.same
      }

}
