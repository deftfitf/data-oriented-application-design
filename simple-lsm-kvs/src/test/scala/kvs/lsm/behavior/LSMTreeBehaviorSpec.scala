package kvs.lsm.behavior

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.DispatcherSelector
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import kvs.lsm.behavior.LSMTreeBehavior.Response
import kvs.lsm.sstable.SSTable.{Got, SSTableReader}
import kvs.lsm.sstable.{SSTable, SSTableFactory}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.specification.Scope

import scala.concurrent.duration._

class LSMTreeBehaviorSpec extends Specification with Mockito {

  val classLoader = getClass.getClassLoader

  "startup test" should {

    trait context extends Scope {
      val testResourceDirectory =
        classLoader.getResource("test/behavior").getPath
      val statisticsFilePath = s"$testResourceDirectory/statistics.txt"
      val writeAheadLogPath = s"$testResourceDirectory/write_ahead_log.txt"
      implicit val timeout: Timeout = Timeout(3.seconds)
      val sSTableFactory = mock[SSTableFactory]
      val testKit = ActorTestKit()

      val factoryProbe =
        testKit.createTestProbe[SSTableFactoryBehavior.Command]()
      val factoryBehavior =
        Behaviors.monitor(factoryProbe.ref,
                          SSTableFactoryBehavior(sSTableFactory, 1))

      val sSTable2, sSTable3, sSTable4 = mock[SSTable]
      sSTableFactory.recovery(2) returns sSTable2
      sSTableFactory.recovery(3) returns sSTable3
      sSTableFactory.recovery(4) returns sSTable4

      val sSTableReader2, sSTableReader3, sSTableReader4 = mock[SSTableReader]
      sSTable2.newReader() returns sSTableReader2
      sSTable3.newReader() returns sSTableReader3
      sSTable4.newReader() returns sSTableReader4

      val lsmTreeProbe = testKit.createTestProbe[LSMTreeBehavior.Command]()
      val lSMTreeBehavior =
        Behaviors.monitor(
          lsmTreeProbe.ref,
          LSMTreeBehavior.apply(factoryBehavior,
                                statisticsFilePath,
                                writeAheadLogPath,
                                DispatcherSelector.sameAsParent()))
      val lsmTree =
        testKit.spawn(lSMTreeBehavior)
    }

    "perform startup test of WAL and SSTable based on statistics" in new context {
      factoryProbe
        .expectMessageType[SSTableFactoryBehavior.Initialize]
        .sequenceNo === 2
      factoryProbe
        .expectMessageType[SSTableFactoryBehavior.Initialize]
        .sequenceNo === 3
      factoryProbe
        .expectMessageType[SSTableFactoryBehavior.Initialize]
        .sequenceNo === 4

      lsmTreeProbe
        .expectMessageType[LSMTreeBehavior.Command.Applied]
        .res
        .sequenceNo === 2
      lsmTreeProbe
        .expectMessageType[LSMTreeBehavior.Command.Applied]
        .res
        .sequenceNo === 3
      lsmTreeProbe
        .expectMessageType[LSMTreeBehavior.Command.Applied]
        .res
        .sequenceNo === 4
    }

    "can read data stored in MemTable reproduced from WAL" in new context {
      val key = "key1"

      lsmTreeProbe.receiveMessages(3)
      val getProbe = testKit.createTestProbe[LSMTreeBehavior.Response]()
      lsmTree ! LSMTreeBehavior.Command.Request.Get("key1", getProbe.ref)
      getProbe.expectMessageType[Response.Got] === Response.Got(
        Got.Found("value1''"))
    }

    "If the key does not exist in MemTable" >> {

      "when first found in the latest SSTable" in new context {
        val key = "key4.5"
        sSTableReader4.get(key) returns Got.Found("value4.5")
        sSTableReader3.get(key) returns Got.Deleted
        sSTableReader2.get(key) returns Got.Found("value4.4")

        lsmTreeProbe.receiveMessages(3)
        val getProbe = testKit.createTestProbe[LSMTreeBehavior.Response]()
        lsmTree ! LSMTreeBehavior.Command.Request.Get("key4.5", getProbe.ref)
        getProbe.expectMessageType[Response.Got] === Response.Got(
          Got.Found("value4.5"))
      }

      "when deleted in the latest SSTable" in new context {
        val key = "key4.5"
        sSTableReader4.get(key) returns Got.Deleted
        sSTableReader3.get(key) returns Got.NotFound
        sSTableReader2.get(key) returns Got.Found("value4.4")

        lsmTreeProbe.receiveMessages(3)
        val getProbe = testKit.createTestProbe[LSMTreeBehavior.Response]()
        lsmTree ! LSMTreeBehavior.Command.Request.Get("key4.5", getProbe.ref)
        getProbe.expectMessageType[Response.Got] === Response.Got(Got.Deleted)
      }

      "when first found in the last SSTable" in new context {
        val key = "key4.5"
        sSTableReader4.get(key) returns Got.NotFound
        sSTableReader3.get(key) returns Got.NotFound
        sSTableReader2.get(key) returns Got.Found("value4.5")

        lsmTreeProbe.receiveMessages(3)
        val getProbe = testKit.createTestProbe[LSMTreeBehavior.Response]()
        lsmTree ! LSMTreeBehavior.Command.Request.Get("key4.5", getProbe.ref)
        getProbe.expectMessageType[Response.Got] === Response.Got(
          Got.Found("value4.5"))
      }

      "if deleted for the first time in the last SSTable" in new context {
        val key = "key4.5"
        sSTableReader4.get(key) returns Got.NotFound
        sSTableReader3.get(key) returns Got.NotFound
        sSTableReader2.get(key) returns Got.Deleted

        lsmTreeProbe.receiveMessages(3)
        val getProbe = testKit.createTestProbe[LSMTreeBehavior.Response]()
        lsmTree ! LSMTreeBehavior.Command.Request.Get("key4.5", getProbe.ref)
        getProbe.expectMessageType[Response.Got] === Response.Got(Got.Deleted)
      }

    }

  }

}
