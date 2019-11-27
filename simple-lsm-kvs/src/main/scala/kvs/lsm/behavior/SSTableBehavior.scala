package kvs.lsm.behavior

import akka.actor.typed._
import akka.actor.typed.scaladsl.{Behaviors, Routers}
import kvs.lsm.sstable.SSTable
import kvs.lsm.sstable.SSTable.{Got, SSTableReader}

object SSTableBehavior {

  final case class Get(key: String, replyTo: ActorRef[Got])

  def pool(sSTable: SSTable, size: Int = 3): Behavior[Get] =
    Routers.pool(poolSize = size)(
      Behaviors
        .supervise(
          Behaviors
            .supervise(worker(sSTable.newReader()))
            .onFailure[java.io.IOException](SupervisorStrategy.restart))
        .onFailure[java.io.IOError](SupervisorStrategy.restart))

  private def worker(sSTableReader: SSTableReader): Behavior[Get] =
    Behaviors.setup { _ =>
      Behaviors
        .receiveMessage[Get] {
          case Get(key, replyTo) =>
            replyTo ! sSTableReader.get(key)
            Behaviors.same
        }
        .receiveSignal {
          case (_, signal) if signal == PreRestart || signal == PostStop =>
            sSTableReader.close()
            Behaviors.same
        }
    }

}
