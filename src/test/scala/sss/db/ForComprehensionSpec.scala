package sss.db

class ForComprehensionSpec extends DbSpecSetup {

  /*it should "" in {

    val time = new Date()
    fixture.table.insert(0, "strId", time, 42)
    val rows = for {
      r <- fixture.table
    } yield(r)

    assert(rows.size === 1, "Should only be one row!")
    val row = rows(0)

    assert(row[String]("strId") == "strId")
    assert(row("createTime") === time.getTime)
    assert(row("intVal") === 42)

  }*/

}