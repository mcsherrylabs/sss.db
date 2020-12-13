package sss.db

import java.util.Date

import org.scalatest._

import scala.util.{Failure, Success, Try}

@DoNotDiscover
class ValidateTransactionSpec extends DbSpecSetup {


  "A Transaction" should "validate without being persisted" in {
    val time = new Date()
    fixture.table.insert(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
    println(s"Max " + fixture.table.maxId)
    fixture.table.validateTx(
      fixture.table.insert(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
    ) match {
      case Failure(_) => fail("Should pass")
      case Success(row) => {
        // l is id of new record, which should not exist
        assert(fixture.table.get(row.id).isEmpty, "Should have rolled back")
      }
    }
    val m = fixture.table.maxId
    println(s"Max now $m" )
    fixture.dbUnderTest.executeSql(s"ALTER TABLE test ALTER COLUMN id RESTART WITH ${m+1};")
    fixture.table.insert(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
    println(s"Max NOW " + fixture.table.maxId)
  }

  "A bad Transaction" should "validate (to failure) without being persisted" in {
    val time = new Date()
    val beforeCount = fixture.table.count
    fixture.table.validateTx {
      val newMaxId = fixture.table.inTransaction {
        val maxId = fixture.table.maxId
        val row = fixture.table.insert(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
        val newMaxId = fixture.table(row.id).id
        assert(maxId + 1 == newMaxId)
        newMaxId
      }
      fixture.table(newMaxId + 1) // <- cannot exist
    } match {
      case Failure(e: DbException) => //println(e)
      case _ => {
        // l is id of new record, which should not exist
        fail("Should have failed")
      }
    }
    assert(beforeCount == fixture.table.count, "Failed should not be present")
  }

  "The table " should "support altering the next id sequence to the max id in table plus 1" in {
    val time = new Date()
    val currentMax = fixture.table.maxId()
    Try (fixture.table.inTransaction {
      fixture.table.insert(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
      fixture.table.insert(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
      fixture.table.insert(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
      throw new RuntimeException("FAIL THIS TX!")
    }) recover{ case e => fixture.table.setNextIdToMaxIdPlusOne()}

    val r = fixture.table.insert(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
    assert(r.id == currentMax + 1, "Failed inserts should not increment the identity sequence ")
  }
}