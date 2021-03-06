package kvs.lsm.statistics

import java.io._

import kvs.lsm.statistics.Statistics.StatisticsIllegalState
import Statistics._
import kvs.lsm.sstable.Logs

case class Statistics private (statisticsFile: File,
                               nextSequenceNo: Int,
                               activeSequenceNo: Seq[Int]) {

  private def formatStatistics(nextSequenceNo: Int,
                               activeSequenceNo: Seq[Int]): String =
    s"$nextSequenceNo ${activeSequenceNo.mkString("[", ",", "]")}"

  @throws[StatisticsIllegalState]
  def updateStatistics(nextSequenceNo: Int, logs: Logs): Unit = {
    if (!statisticsFile.exists())
      throw StatisticsIllegalState(
        s"can't find statistics file: ${statisticsFile.getAbsolutePath}")
    try {
      val writer = new FileWriter(statisticsFile)
      try {
        val formatted = formatStatistics(nextSequenceNo, logs.activeSequenceNo)
        writer.write(formatted)
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

  @throws[StatisticsInitializeError]
  def initialize(statisticsFilePath: String): Statistics =
    try {
      val file = new File(statisticsFilePath)
      if (!file.exists()) {
        if (!file.createNewFile())
          throw StatisticsInitializeError(
            s"can't find, and create statistics file: $statisticsFilePath")
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
        val activeSequenceNo =
          statistics(1).tail.init
            .split(",")
            .filter(_.nonEmpty)
            .map(_.toInt)
            .toSeq
        Statistics(file, nextSequenceNo, activeSequenceNo)
      } finally {
        reader.close()
      }
    } catch {
      case e: Exception => throw StatisticsInitializeError(e.getMessage)
    }

}
