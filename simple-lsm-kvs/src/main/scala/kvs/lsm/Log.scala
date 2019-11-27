package kvs.lsm

import akka.actor.typed.ActorRef
import kvs.lsm.behavior.SSTableBehavior
import kvs.lsm.sstable.SSTable
import kvs.lsm.sstable.SSTable.{Got, Value}

import scala.collection.mutable

sealed trait Log

object Log {

  case class MemTable(private val index: mutable.TreeMap[String, Value],
                      private val maxSize: Int = 10000)
      extends Log {

    def iterator: Iterator[(String, Value)] = index.iterator

    def isOverMaxSize: Boolean = index.size >= maxSize

    def get(key: String): Got =
      index get key map (_.toGot) getOrElse Got.NotFound

    def set(key: String, value: String): Unit =
      index.update(key, Value.Exist(value))

    def del(key: String): Unit = index.update(key, Value.Deleted)

  }

  object MemTable {

    def empty: MemTable =
      MemTable(mutable.TreeMap[String, Value]()(Ordering.String))

  }

  case class SSTableRef(sSTable: SSTable,
                        routerRef: ActorRef[SSTableBehavior.Get])
      extends Log

}
