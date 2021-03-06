package kvs.lsm

import akka.actor.typed.{ActorSystem, Behavior, DispatcherSelector}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import kvs.lsm.behavior.{LSMTreeBehavior, SSTableFactoryBehavior}
import kvs.lsm.sstable.SSTableFactory

object ConsoleClient {

  val SPARSE_INDEX_PER = 100
  val WRITE_AHEAD_LOG_PATH = "data/simplekvs/lsm/write_ahead_log.txt"
  val SEGMENT_FILE_BATH_PATH = "data/simplekvs/lsm"
  val STATISTICS_FILE_PATH = "data/simplekvs/lsm/statistics.txt"
  val SSTABLE_READER_POOL_SIZE = 3

  def main(args: Array[String]): Unit = {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(3.seconds)
    val sSTableFactory =
      new SSTableFactory(sparseIndexPer = SPARSE_INDEX_PER,
                         segmentFileBathPath = SEGMENT_FILE_BATH_PATH)
    val factoryBehavior =
      SSTableFactoryBehavior(sSTableFactory, SSTABLE_READER_POOL_SIZE)
    val system = ActorSystem(
      LSMTreeBehavior(
        factoryBehavior,
        statisticsFilePath = STATISTICS_FILE_PATH,
        writeAheadLogPath = WRITE_AHEAD_LOG_PATH,
        DispatcherSelector.fromConfig("blocking-io-dispatcher")
      ),
      "lsm-tree"
    )
    sys.addShutdownHook {
      system.terminate()
    }

    def result: Behavior[LSMTreeBehavior.Response] = {
      Behaviors.receiveMessage {
        case LSMTreeBehavior.Response.UnInitialized =>
          println("uninitialized"); Behaviors.same
        case LSMTreeBehavior.Response.Deleted =>
          println("deleted"); Behaviors.same
        case LSMTreeBehavior.Response.Got(v) =>
          println(s"got: $v"); Behaviors.same
        case LSMTreeBehavior.Response.Set => println("set"); Behaviors.same
      }
    }

    val r = system.systemActorOf(result, "r")

    val sc = new java.util.Scanner(System.in)
    while (sc.hasNextLine()) {
      try {
        val cmd = sc.nextLine().trim.split("\\s+")
        cmd(0).toLowerCase match {
          case "get" => system ! LSMTreeBehavior.Command.Request.Get(cmd(1), r)
          case "set" =>
            system ! LSMTreeBehavior.Command.Request.Set(cmd(1), cmd(2), r)
          case "del"  => system ! LSMTreeBehavior.Command.Request.Del(cmd(1), r)
          case "exit" => sys.exit()
        }
      } catch {
        case e: Throwable =>
          println(s"Error: ${e.getMessage}")
      }
    }
  }

}
