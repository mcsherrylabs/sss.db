package sss.db

import java.lang.RuntimeException
import java.util.Date
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Try}



@DoNotDiscover
class ValidateTransactionSpec extends DbSpecSetup {


  "A Transaction" should "validate without being persisted" in {

    val db = fixture.dbUnderTest
    import db.runContext
    import db.runContext.ds
    import db.runContext.ec

    val time = new Date()

    val eventualRow = fixture.table.insert(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 67)).runRollback

    val assertedRow = eventualRow map { r =>
      // is id of new record, which should not exist
      assert (fixture.table.get(r.id).runSyncUnSafe.isEmpty, "Should have rolled back")
    }

    Await.result(assertedRow, 1.second)
  }

  "A bad Transaction" should "validate (to failure) without being persisted" in {
    val time = new Date()

    val db = fixture.dbUnderTest
    import db.runContext

    import db.runContext.ds


    val f = for {
      maxId <- fixture.table.maxId()
      row: Row <- fixture.table.insert(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 67))
      newMaxIdRow: Row <- fixture.table(row.id)
      newMaxId = newMaxIdRow.id
      _ = assert(maxId + 1 == newMaxId)
      failure <- fixture.table(newMaxId + 1) // <- cannot exist
    } yield ()

    Try(Await.result(f.run, 1.second))

    val r = fixture.table.get(1).runSyncUnSafe
    assert (r.isEmpty, "Should have rolled back")

  }

  "The table " should "support altering the next id sequence to the max id in table plus 1" in {
    val time = new Date()

    val db = fixture.dbUnderTest
    import db.runContext
    import db.runContext.ds
    import db.runContext.ec

    val currentMax = fixture.table.maxId().runSyncUnSafe



    val inserts = for {
      _ <- fixture.table.insert(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 67))
      _ <- fixture.table.insert(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 67))
      _ <- fixture.table.insert(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 67))
      _ = throw new RuntimeException("FAIL THIS TX!")
    } yield ()


    val result = inserts.run recoverWith {
      case e => fixture.table.setNextIdToMaxIdPlusOne().run
    } flatMap { _ =>
      fixture.table.insert(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 67)).run
    }

    val r = Await.result(result, 1.second)
    assert(r.id == currentMax + 1, "Failed inserts should not increment the identity sequence ")
  }
}
