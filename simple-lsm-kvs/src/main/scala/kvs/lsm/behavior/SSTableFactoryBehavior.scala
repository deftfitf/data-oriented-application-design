package kvs.lsm.behavior

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import kvs.lsm.sstable.Log.{MemTable, SSTableRef}
import kvs.lsm.sstable.{SSTable, SSTableFactory, SSTableMergeIterator}

import scala.concurrent.duration._

object SSTableFactoryBehavior {

  sealed trait Command
  final case class Apply(sequenceNo: Int,
                         memTable: MemTable,
                         replyTo: ActorRef[Applied])
      extends Command
  final case class Initialize(sequenceNo: Int, replyTo: ActorRef[Applied])
      extends Command
  final case class Stop(actorRef: ActorRef[SSTableBehavior.Get]) extends Command
  final case class Merge(newSequenceNo: Int,
                         mergeSSTables: Seq[SSTable],
                         replyTo: ActorRef[Merged])
      extends Command

  final case class Merged(mergedSegment: SSTableRef,
                          removedSequenceNo: Seq[Int])

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
        case Merge(newSequenceNo, mergeSSTables, replyTo) =>
          val iterator = SSTableMergeIterator(mergeSSTables)
          val mergedSSTable = sSTableFactory.apply(newSequenceNo, iterator)
          val sSTableRef = context.spawnAnonymous(
            SSTableBehavior.pool(mergedSSTable, readerPoolSize))

          replyTo ! Merged(SSTableRef(mergedSSTable, sSTableRef),
                           mergeSSTables.map(_.sequenceNo))
          Behaviors.same
        case Stop(actorRef) =>
          context.stop(actorRef)
          Behaviors.same
      }
    }

}
