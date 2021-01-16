package sss.db

import org.scalatest._
import sss.db.datasource.DataSource
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}

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

}

trait DbSpecSetup extends AnyFlatSpec with DbSpecSetupBase
trait AsyncDbSpecSetup extends AsyncFlatSpec with DbSpecSetupBase
