package kvs.lsm.statistics

import java.io.{File, FileWriter}

import kvs.lsm.Log.MemTable

import scala.io.Source

case class Statistics private (lastSequenceNo: Int,
                               activeSequenceNos: Seq[Int]) {

  def recoveryMemTable(): MemTable = ???

}

object Statistics {

  final private val STATISTICS_FILE =
    "data/simplekvs/lsm/statistics.txt"

  def initialize(): Statistics = {
    val file = new File(STATISTICS_FILE)
    if (!file.exists()) {
      file.createNewFile()
      val w = new FileWriter(file)
      w.write("0")
      w.flush()
      w.close()
    }
    val reader = Source.fromFile(file).bufferedReader()

    try {
      val statistics = reader.readLine().split("\\s+")
      val lastSequenceNo = statistics(0).toInt
      val activeSequenceNos =
        if (lastSequenceNo > 0) statistics(1).split(",").map(_.toInt).toList
        else Nil
      Statistics(lastSequenceNo = lastSequenceNo,
                 activeSequenceNos = activeSequenceNos)
    } finally {
      reader.close()
    }
  }

}
