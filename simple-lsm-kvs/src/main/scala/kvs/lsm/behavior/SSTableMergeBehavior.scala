package kvs.lsm.behavior

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import kvs.lsm.sstable.Log.SSTableRef
import kvs.lsm.sstable.{SSTable, SSTableFactory, SSTableMergeIterator}

import scala.concurrent.duration._

object SSTableMergeBehavior {

  final case class Merge(newSequenceNo: Int,
                         mergeSSTables: Seq[SSTable],
                         replyTo: ActorRef[Merged])
  final case class Merged(mergedSegment: SSTableRef,
                          removedSequenceNo: Seq[Int])

  final case object MergeException extends Throwable

  def apply(sSTableFactory: SSTableFactory,
            readerPoolSize: Int): Behavior[Merge] =
    Behaviors
      .supervise(behavior(sSTableFactory, readerPoolSize))
      .onFailure[Throwable](SupervisorStrategy.restart
        .withLimit(maxNrOfRetries = 3, withinTimeRange = 3.seconds))

  private def behavior(sSTableFactory: SSTableFactory,
                       readerPoolSize: Int): Behavior[Merge] =
    Behaviors.receive[Merge] { (context, msg) =>
      msg match {
        case Merge(newSequenceNo, mergeSSTables, replyTo) =>
          val iterator = SSTableMergeIterator(mergeSSTables)
          val mergedSSTable = sSTableFactory.apply(newSequenceNo, iterator)
          val sSTableRef = context.spawnAnonymous(
            SSTableBehavior.pool(mergedSSTable, readerPoolSize))

          replyTo ! Merged(SSTableRef(mergedSSTable, sSTableRef),
                           mergeSSTables.map(_.sequenceNo))
          Behaviors.same
      }
    }

}
