package sss.db

class DbIsolateSpec extends DbSpecSetup {

  /*it should " be able to delete a row " in {

    val time = new Date()
    val r1 = fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45)))
    val r2 = fixture.table.find(Where("id = ?", r1("id")))
    assert(r1 == r2.get)
    val deletedRowCount = fixture.table.delete(where(ps"id = ${r1("id")}"))
    assert(deletedRowCount === 1)
    assert(fixture.table.find(where("id = ?", r1("id"))) == None)
  }*/

}