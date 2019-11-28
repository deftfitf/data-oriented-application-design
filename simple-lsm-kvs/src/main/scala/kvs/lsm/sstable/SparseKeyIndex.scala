package kvs.lsm.sstable

import kvs.lsm.sstable.SparseKeyIndex.Position

import scala.annotation.tailrec

/**
  * 疎なキーのインデックス
  * 任意のキー sparseKeys(i) の キー位置は keyPosition(i)
  *
  * @param sparseKeys
  * @param keyPositions
  */
case class SparseKeyIndex(sparseKeys: IndexedSeq[String],
                          keyPositions: IndexedSeq[Long]) {

  /**
    * キーが存在する可能性がある範囲を調べる
    * @param key 存在を確認するキー
    * @return 存在する可能性がある範囲
    *         確実に存在する場合と, 最後の範囲に存在する可能性がある場合には
    *         タプルの二つ目がNoneになる.
    *         前者であれば, 最初のキーで発見できる
    *         後者であれば, EOFまで検査する必要がある.
    */
  def positionRange(key: String): Position = {
    @tailrec
    def binarySearch(l: Int, r: Int): Int =
      if (r - l > 1) {
        val center = l + (r - l) / 2
        if (sparseKeys(center) > key) binarySearch(l, center)
        else binarySearch(center, r)
      } else l

    val left = binarySearch(-1, sparseKeys.size)
    if (left == -1)
      Position.NotFound
    else if (sparseKeys(left) == key)
      Position.Found(keyPositions(left))
    else if (left < sparseKeys.size - 1)
      Position.Range(keyPositions(left), keyPositions(left + 1))
    else
      Position.Tail(keyPositions(left))
  }

}

object SparseKeyIndex {

  sealed trait Position
  object Position {
    final case object NotFound extends Position
    final case class Found(start: Long) extends Position
    final case class Range(start: Long, end: Long) extends Position
    final case class Tail(start: Long) extends Position
  }

}
