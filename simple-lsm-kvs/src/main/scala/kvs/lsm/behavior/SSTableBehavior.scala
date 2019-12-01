package kvs.lsm.behavior

import akka.actor.typed._
import akka.actor.typed.scaladsl.{Behaviors, Routers}
import kvs.lsm.sstable.SSTable
import kvs.lsm.sstable.SSTable.Got

import scala.concurrent.duration._

object SSTableBehavior {

  final case class Get(key: String, replyTo: ActorRef[Got])

  def pool(sSTable: SSTable, size: Int = 3): Behavior[Get] =
    Routers.pool(poolSize = size)(
      Behaviors
        .supervise(worker(sSTable))
        .onFailure[Throwable](SupervisorStrategy.restart
          .withLimit(maxNrOfRetries = 3, withinTimeRange = 3.seconds)))

  private def worker(sSTable: SSTable): Behavior[Get] =
    Behaviors.setup { _ =>
      val sSTableReader = sSTable.newReader()
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
