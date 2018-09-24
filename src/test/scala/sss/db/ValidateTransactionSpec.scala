package sss.db

import java.util.Date

import org.scalatest._

import scala.util.{Failure, Success}

@DoNotDiscover
class ValidateTransactionSpec extends DbSpecSetup {


  "A Transaction" should "validate without being persisted" in {
    val time = new Date()
    fixture.table.validateTx(
      fixture.table.insert(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
    ) match {
      case Failure(_) => fail("Should pass")
      case Success(row) => {
        // l is id of new record, which should not exist
        assert(fixture.table.get(row.id).isEmpty, "Should have rolled back")
      }
    }
  }

  "A bad Transaction" should "validate (to failure) without being persisted" in {
    val time = new Date()
    fixture.table.validateTx {
      val maxId = fixture.table.maxId
      val row = fixture.table.insert(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
      val newMaxId = fixture.table(row.id).id
      assert(maxId + 1 == newMaxId)
      fixture.table(newMaxId + 1) // <- cannot exist
    } match {
      case Failure(e: DbException) => //println(e)
      case _ => {
        // l is id of new record, which should not exist
        fail("Should have failed")
      }
    }
  }
}