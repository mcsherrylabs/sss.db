package sss.db

import org.scalatest._
import sss.db.datasource.DataSource
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}

import java.util.Date

trait DbSpecSetupBase extends Matchers with BeforeAndAfterEach {

  self: Suite =>

  case class TestFixture(dbUnderTest: Db, table: Table)

  val dbConfigName = "testDb"

  var fixture: TestFixture = _

  val idCol = "id"
  val statusCol = "status_col"
  val testPaged = "testPaged"
  val testPaged2 = "testPaged2"
  val testRowSerialize = "testRowSerialize"

  override def beforeEach(): Unit = {
    val dbUnderTest = Db(dbConfigName, DataSource(s"$dbConfigName.datasource"))
    fixture = TestFixture(dbUnderTest, dbUnderTest.table("test"))
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    val db = fixture.dbUnderTest
    import db.syncRunContext
    try super.afterEach()
    finally fixture.dbUnderTest.shutdown.runSyncAndGet
  }

  def persistIntVal(intVal: Int): FutureTx[Row] = fixture.table.persist(Map("strId" -> "strId", "createTime" -> new Date(), "intVal" -> intVal))
  def persistIntVals(intVals: Seq[Int]): FutureTx[Seq[Row]] = FutureTx.sequence(intVals map persistIntVal)
}

trait DbSpecSetup extends AnyFlatSpec with DbSpecSetupBase

trait DbSpecQuickSetupBase extends DbSpecSetupBase {
  self: Suite =>

  var db: Db = _
  var table: Table = _
  implicit var syncRunContext: SyncRunContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    db = fixture.dbUnderTest
    table = fixture.table
    syncRunContext = db.syncRunContext
  }

  override def afterEach(): Unit = super.afterEach()
}

trait DbSpecQuickSetup extends AnyFlatSpec with DbSpecQuickSetupBase

trait AsyncDbSpecSetup extends AsyncFlatSpec with DbSpecSetupBase
