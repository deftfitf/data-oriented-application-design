package kvs

import org.specs2.matcher.Scope
import org.specs2.mutable.{After, Specification}

class SimpleKvsSpec extends Specification {

  sequential

  trait context extends Scope with After {

    private val testDatabaseFileName = "data/simplekvs/test_database.txt"
    private val file = new java.io.File(testDatabaseFileName)
    if (file.exists()) file.delete()

    val simpleKvs = SimpleKvs(testDatabaseFileName)

    override def after: Unit = {
      if (file.exists()) {
        val _ = file.delete()
      }
    }

  }

  "#set" should {

    "あるキーに値が存在しない場合, そのキーに対してsetすると, キーの値として設定される" in new context {
      val key = "key"
      val value = "value"

      simpleKvs.get(key) must beNone
      simpleKvs.set(key, value)
      simpleKvs.get(key) must beSome("value")
    }

    "あるキーに値が存在する場合, そのキーに対してsetをすると, キーの最新の値として上書き設定される" in new context {
      val key = "key"
      val value = "value"
      val afterValue = "after_value"

      simpleKvs.set(key, value)
      simpleKvs.get(key) must beSome("value")
      simpleKvs.set(key, afterValue)
      simpleKvs.get(key) must beSome("after_value")
    }

  }

  "#get" should {

    "あるキーに値が存在しない場合, そのキーに対してgetをすると, Noneが返る" in new context {
      val key = "key"

      simpleKvs.get(key) must beNone
    }

    "あるキーに値が存在する場合, そのキーに対してgetをすると, Some(設定されている最新の値)が返る" in new context {
      val key = "key"
      val value = "value"
      val afterValue = "after_value"

      simpleKvs.set(key, value)
      simpleKvs.set(key, afterValue)
      simpleKvs.get(key) must beSome("after_value")
    }

  }

  "#del" should {

    "削除対象のキーに値が存在しない場合, 何もしない" in new context {
      simpleKvs.set("key1", "value1")
      simpleKvs.set("key2", "value2")
      simpleKvs.set("key3", "value3")

      simpleKvs.del("key4")

      simpleKvs.get("key1") must beSome("value1")
      simpleKvs.get("key2") must beSome("value2")
      simpleKvs.get("key3") must beSome("value3")
    }

    "削除対象のキーに値が存在する場合, 値を削除する" in new context {
      simpleKvs.set("key1", "value1")
      simpleKvs.set("key2", "value2")
      simpleKvs.set("key1", "value1_updated")

      simpleKvs.del("key1")

      simpleKvs.get("key1") must beNone
    }

  }

}
