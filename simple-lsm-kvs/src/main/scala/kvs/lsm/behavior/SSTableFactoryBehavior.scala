package kvs.lsm.behavior

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import kvs.lsm.Log.{MemTable, SSTableRef}
import kvs.lsm.sstable.SSTableFactory

import scala.concurrent.duration._

object SSTableFactoryBehavior {

  sealed trait Command
  final case class Apply(sequenceNo: Int,
                         memTable: MemTable,
                         replyTo: ActorRef[Applied])
      extends Command
  final case class Initialize(sequenceNo: Int, replyTo: ActorRef[Applied])
      extends Command
  final case class Shutdown(actorRef: ActorRef[SSTableBehavior.Get])
      extends Command

  final case class Applied(sequenceNo: Int, sSTableRef: SSTableRef)

  def apply(sSTableFactory: SSTableFactory,
            readerPoolSize: Int): Behavior[Command] =
    Behaviors
      .supervise(behavior(sSTableFactory, readerPoolSize))
      .onFailure[Throwable](SupervisorStrategy.restart
        .withLimit(maxNrOfRetries = 3, withinTimeRange = 3.seconds))

  private def behavior(sSTableFactory: SSTableFactory,
                       readerPoolSize: Int): Behavior[Command] =
    Behaviors.receive[Command] { (context, msg) =>
      msg match {
        case Initialize(sequenceNo, replyTo) =>
          val sSTable = sSTableFactory.recovery(sequenceNo)
          val sSTableRef = context.spawnAnonymous(
            SSTableBehavior.pool(sSTable, readerPoolSize))

          replyTo ! Applied(sequenceNo, SSTableRef(sSTable, sSTableRef))
          Behaviors.same
        case Apply(sequenceNo, memTable, replyTo) =>
          val sSTable = sSTableFactory.apply(sequenceNo, memTable.iterator)
          val sSTableRef = context.spawnAnonymous(
            SSTableBehavior.pool(sSTable, readerPoolSize))

          replyTo ! Applied(sequenceNo, SSTableRef(sSTable, sSTableRef))
          Behaviors.same
        case Shutdown(actorRef) =>
          context.stop(actorRef)
          Behaviors.same
      }
    }

}
