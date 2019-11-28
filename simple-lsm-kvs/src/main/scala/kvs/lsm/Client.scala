package kvs.lsm

import akka.actor.typed.{ActorSystem, Behavior, DispatcherSelector}
import akka.actor.typed.scaladsl.Behaviors
import kvs.lsm.behavior.LSMTreeBehavior
import kvs.lsm.sstable.SSTableFactory

object Client {

  def main(args: Array[String]): Unit = {
    val sSTableFactory = new SSTableFactory
    val system = ActorSystem(
      LSMTreeBehavior(sSTableFactory,
                      readerPoolSize = 3,
                      DispatcherSelector.fromConfig("blocking-io-dispatcher")),
      "lsm-tree")
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
      val cmd = sc.nextLine().trim.split("\\s+")
      cmd(0).toLowerCase match {
        case "get" => system ! LSMTreeBehavior.Command.Request.Get(cmd(1), r)
        case "set" =>
          system ! LSMTreeBehavior.Command.Request.Set(cmd(1), cmd(2), r)
        case "del" => system ! LSMTreeBehavior.Command.Request.Del(cmd(1), r)
      }
    }
  }

}
