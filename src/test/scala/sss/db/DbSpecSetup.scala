package sss.db

import org.scalatest._

trait DbSpecSetupBase extends Matchers with BeforeAndAfter {

  self: Suite =>

  case class TestFixture(dbUnderTest: Db, table: Table)

  val dbConfigName = "testDb"
  var fixture: TestFixture = _

  val idCol = "id"
  val statusCol = "status_col"
  val testPaged = "testPaged"
  val testPaged2 = "testPaged2"

  before {
    val dbUnderTest = Db(dbConfigName)(Db.defaultDataSource(dbConfigName))
    fixture = TestFixture(dbUnderTest, dbUnderTest.table("test"))
  }

  after {
    fixture.dbUnderTest.shutdown
  }

}

trait DbSpecSetup extends FlatSpec with DbSpecSetupBase
trait AsyncDbSpecSetup extends AsyncFlatSpec with DbSpecSetupBase

