package kvs.lsm.sstable

import akka.actor.typed.Scheduler
import kvs.lsm.behavior.SSTableBehavior
import kvs.lsm.sstable.Log.{MemTable, SSTableRef}

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.concurrent.{ExecutionContext, Future}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout

case class Logs(underlying: SortedMap[Int, Log]) {

  def updated(sequenceNo: Int, log: Log): Logs =
    copy(underlying = underlying.updated(sequenceNo, log))

  def activeSequenceNos: Seq[Int] =
    underlying.values.collect {
      case SSTableRef(sSTable, _) => sSTable.sequenceNo
    }.toSeq

  def merged(removedSequenceNos: Seq[Int], mergedSStableRef: SSTableRef): Logs =
    copy(
      underlying = removedSequenceNos
        .foldLeft(underlying)(_ removed _)
        .updated(mergedSStableRef.sSTable.sequenceNo, mergedSStableRef))

  def read(key: String)(implicit ec: ExecutionContext,
                        t: Timeout,
                        s: Scheduler): Future[SSTable.Got] =
    underlying.values.foldLeft(
      Future.successful(SSTable.Got.NotFound): Future[SSTable.Got]) {
      (fValue, log) =>
        for {
          value <- fValue
          res <- value match {
            case SSTable.Got.NotFound =>
              log match {
                case memTable: MemTable =>
                  Future.successful(memTable get key)
                case sSTableRef: SSTableRef =>
                  sSTableRef.routerRef
                    .ask[SSTable.Got](SSTableBehavior.Get(key, _))
              }
            case _ => Future.successful(value)
          }
        } yield res
    }

  def mergeableSSTables: Seq[SSTable] =
    underlying
      .takeWhile(_._2.isInstanceOf[SSTable])
      .map[SSTable](_._2.asInstanceOf[SSTable])
      .toSeq

}

object Logs {

  def empty: Logs = Logs(TreeMap[Int, Log]()(Ordering.Int.reverse))

}
