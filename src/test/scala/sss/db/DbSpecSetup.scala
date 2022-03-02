package sss.db

import org.scalatest._
import sss.db.datasource.DataSource
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}

import java.util.Date

trait DbSpecSetupBase extends Matchers with BeforeAndAfter {

  self: Suite =>

  case class TestFixture(dbUnderTest: Db, table: Table)

  val dbConfigName = "testDb"

  var fixture: TestFixture = _

  val idCol = "id"
  val statusCol = "status_col"
  val testPaged = "testPaged"
  val testPaged2 = "testPaged2"
  val testRowSerialize = "testRowSerialize"

  before {
    val dbUnderTest = Db(dbConfigName, DataSource(s"$dbConfigName.datasource"))
    fixture = TestFixture(dbUnderTest, dbUnderTest.table("test"))
  }

  after {
    val db = fixture.dbUnderTest
    import db.syncRunContext
    import db.syncRunContext.executor
    fixture.dbUnderTest.shutdown.runSyncAndGet
  }

  def persistIntVal(intVal: Int): FutureTx[Row] = fixture.table.persist(Map("strId" -> "strId", "createTime" -> new Date(), "intVal" -> intVal))
  def persistIntVals(intVals: Seq[Int]): FutureTx[Seq[Row]] = FutureTx.sequence(intVals map persistIntVal)
}

trait DbSpecSetup extends AnyFlatSpec with DbSpecSetupBase
trait AsyncDbSpecSetup extends AsyncFlatSpec with DbSpecSetupBase
