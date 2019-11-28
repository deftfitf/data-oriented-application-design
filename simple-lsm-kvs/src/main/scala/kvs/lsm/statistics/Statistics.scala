package kvs.lsm.statistics

import java.io._

import kvs.lsm.statistics.Statistics.StatisticsIllegalState

import Statistics._

case class Statistics private (statisticsFile: File,
                               nextSequenceNo: Int,
                               activeSequenceNos: Seq[Int]) {

  @throws[StatisticsIllegalState]
  def updateStatistics(nextSequenceNo: Int,
                       activeSequenceNos: Seq[Int]): Unit = {
    if (!statisticsFile.exists())
      throw StatisticsIllegalState(
        s"can't find statistics file: $STATISTICS_FILE")
    try {
      val writer = new FileWriter(statisticsFile)
      try {
        writer.write(
          s"$nextSequenceNo ${activeSequenceNos.mkString("[", ",", "]")}")
        writer.flush()
      } finally {
        writer.close()
      }
    } catch {
      case e: Throwable => throw StatisticsUpdateError(e.getMessage)
    }
  }

}

object Statistics {

  final case class StatisticsUpdateError(message: String)
      extends Throwable(message)
  final case class StatisticsIllegalState(message: String)
      extends Throwable(message)
  final case class StatisticsInitializeError(message: String)
      extends Throwable(message)

  final private val STATISTICS_FILE =
    "data/simplekvs/lsm/statistics.txt"

  @throws[StatisticsInitializeError]
  def initialize(): Statistics =
    try {
      val file = new File(STATISTICS_FILE)
      if (!file.exists()) {
        if (!file.createNewFile())
          throw StatisticsInitializeError(
            s"can't find, and create statistics file: $STATISTICS_FILE")
        val writer = new FileWriter(file)
        try {
          writer.write("0 []")
          writer.flush()
        } finally {
          writer.close()
        }
      }

      val reader = new BufferedReader(new FileReader(file))
      try {
        val statistics = reader.readLine().split("\\s+")
        val nextSequenceNo = statistics(0).toInt
        val activeSequenceNos =
          statistics(1).tail.init.split(",")
            .filter(_.nonEmpty).map(_.toInt).toSeq
        Statistics(file, nextSequenceNo, activeSequenceNos)
      } finally {
        reader.close()
      }
    } catch {
      case e: Exception => throw StatisticsInitializeError(e.getMessage)
    }

}
