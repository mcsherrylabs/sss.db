package sss.db

import java.util.Date

import org.scalatest.DoNotDiscover

@DoNotDiscover
class DbV1Spec extends DbSpecSetup {


  "A Db" should " allow insert into existing table " in {
    val db = fixture.dbUnderTest
    import db.runContext.ds

    val numInserted = fixture.table.insert(0, "strId", new Date().getTime, 42).runSync.get
    assert(numInserted == 1, s"Should be 1 row created not ${numInserted}!")
  }

  it should " be able to read all rows from a table " in {
    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()
    val rowsT = for {
      r <- fixture.table.insert(0, "strId", time, 42)
      rows <- fixture.table.map(r => r)
    } yield rows

    val rows = rowsT.runSync.get
    assert(rows.size === 1, "Should only be one row!")
    val row = rows(0)

    assert(row[String]("strId") == "strId")
    assert(row("createTime") === time.getTime)
    assert(row("intVal") === 42)
  }



}