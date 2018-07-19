package sss.db

import org.scalatest._

trait DbSpecSetup extends FlatSpec with Matchers with BeforeAndAfter  {

  case class TestFixture(dbUnderTest: Db, table: Table)

  var fixture: TestFixture = _

  val idCol = "id"
  val statusCol = "status_col"
  val testPaged = "testPaged"
  val testPaged2 = "testPaged2"

  before {
    val dbUnderTest = Db("testDb")
    fixture = TestFixture(dbUnderTest, dbUnderTest.table("test"))
  }

  after {
    fixture.dbUnderTest.shutdown
  }

}
