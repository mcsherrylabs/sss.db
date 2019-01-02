package sss.db

import java.util.Date

import org.scalatest.DoNotDiscover

@DoNotDiscover
class DbV1Spec extends DbSpecSetup {

  "A Db" should " allow insert into existing table " in {

    val numInserted = fixture.table.insert(0, "strId", new Date().getTime, 42)
    assert(numInserted == 1, s"Should be 1 row created not ${numInserted}!")
  }

  it should " be able to read all rows from a table " in {

    val time = new Date()
    fixture.table.insert(0, "strId", time, 42)
    val rows = fixture.table.map(r => r)
    assert(rows.size === 1, "Should only be one row!")
    val row = rows(0)

    assert(row[String]("strId") == "strId")
    assert(row("createTime") === time.getTime)
    assert(row("intVal") === 42)
  }

  it should " be able to read all rows from a table (in order!) " in {

    val time = new Date()
    (0 to 10).map (fixture.table.insert(0, "strId", time, _))
    var intVal = 10
    fixture.table.map (r => {

      assert(intVal == r[Int]("intVal"))
      intVal -= 1
    }, OrderDesc("intVal"))

    intVal = 0
    fixture.table.map (r => {

      assert(intVal == r[Int]("intVal"))
      intVal += 1
    }, OrderAsc("intVal"))

  }

  it should " be able to find the row inserted " in {

    val time = new Date()
    fixture.table.insert(0, "strId", time, 45)
    val rows = fixture.table.filter(where(ps"createTime = ${time.getTime}"))
    assert(rows.size === 1, "Should only be one row found !")
    val row = rows(0)
    assert(row("strId") === "strId")
    assert(row("createTime") === time.getTime)
    assert(row("intVal") === 45)
  }

  it should " support shorthand filter" in {

    val time = new Date()
    fixture.table.insert(3456, "strId", time, 45)
    val rows = fixture.table.filter("createTime" -> time)
    assert(rows.size === 1, "Should only be one row found !")
    val row = rows(0)
    assert(row("strId") === "strId")
    assert(row("createTime") === time.getTime)
    assert(row("intVal") === 45)
  }

  it should " be able to find the row inserted by id " in {

    val time = new Date()
    fixture.table.insert(99, "strId", time, 45)
    fixture.table.get(99) match {
      case None => fail("oh oh, failed to find row by id")
      case Some(r) => assert(r("id") === 99)
    }
  }

  it should " be able to find the row searching by field name " in {

    val time = new Date()
    fixture.table.insert(99, "strId", time, 45)
    fixture.table.find(where(s"id = ?", 99)) match {
      case None => fail("oh oh, failed to find row by id")
      case Some(r) => assert(r("id") === 99)
    }
  }

  it should " support shorthand find " in {

    val time = new Date()
    fixture.table.insert(100, "strId", time, 45)
    fixture.table.find("id" -> 100) match {
      case None => fail("oh oh, failed to find row by id")
      case Some(r) => assert(r("id") === 100)
    }
    fixture.table.find("id" -> 100, "strId" -> "strId", "createTime" -> time) match {
      case None => fail("oh oh, failed to find row ")
      case Some(r) => assert(r("createTime") === time.getTime)
    }
  }

  it should " not be able to find a single row when 2 are present " in {

    val time = new Date()
    fixture.table.insert(99, "strId", time, 45)
    fixture.table.insert(100, "strId", time, 45)
    try {
      fixture.table.find(where(s"strId = ?", "strId"))
      fail("there are 2 rows with strId,  should throw ...")
    } catch {
      case e: Error =>
    }
  }

  it should " support a transaction " in {
    val time = new Date()
    try {
      fixture.table.inTransaction {
        fixture.table.insert(999999, "strId", time, 45)
        throw new Error("Ah HA!")

      }
    } catch {
      case e: Error => println(e)
    }

    try {
      fixture.table.find(idCol -> 999999) match {
        case Some(r) => fail("there is a row with 999999,  should have thrown ex ...")
        case x =>
      }

    } catch {
      case e: Error =>
    }

    try {
      fixture.table.inTransaction {
        fixture.table.insert(999999, "strId", time, 45)
      }
    } catch {
      case e: Error => println(e)
    }

    try {
      fixture.table.find(where(ps"id = ${999999}")) match {
        case Some(r) =>
        case x => fail("there is no row with 999999...")
      }

    } catch {
      case e: Error =>
    }
  }

  it should " support a transaction from db level " in {
    val time = new Date()
    try {
      fixture.dbUnderTest.inTransaction {
        fixture.table.insert(999999, "strId", time, 45)
        throw new Error("Ah HA!")

      }
    } catch {
      case e: Error => println(e)
    }

    fixture.table.find(where("id = ?", 999999)) match {
      case Some(r) => fail("there is a row with 999999,  should have thrown ex ...")
      case x =>
    }

    fixture.dbUnderTest.inTransaction {
      fixture.table.insert(999999, "strId", time, 45)

      fixture.table.find(where("id = ?", 999999))
        .getOrElse(fail("there is no row with 999999..."))
    }
  }

  it should " honour executeSql fail from db level transaction " in {
    val time = new Date()
    try {
      fixture.dbUnderTest.inTransaction {
        val n = fixture.table.name
        fixture.dbUnderTest.executeSql(s"INSERT INTO $n VALUES (999999, 'strId', ${time.getTime}, 45)")
        throw new Error("Ah HA!")

      }
    } catch {
      case e: Error => println(e)
    }


    assert(fixture.table.find(where("id = ?", 999999)).isEmpty,
      "there is a row with 999999,  should have thrown ex ...")

  }
  it should " honour executeSql from db level transaction " in {
    val time = new Date()
    val n = fixture.table.name

    fixture.dbUnderTest.inTransaction {
      fixture.dbUnderTest.executeSql(s"INSERT INTO $n VALUES (999999, 'strId', ${time.getTime}, 45)")
    }

    fixture.table.find(where("id = ?", 999999)) match {
      case Some(r) =>
      case None => fail("there is no row with 999999.")
    }

    fixture.dbUnderTest.executeSql(s"INSERT INTO $n VALUES (199999, 'strId', ${time.getTime}, 45)")
    fixture.table.find(where("id = ?", 199999)) match {
      case Some(r) =>
      case None => fail("there is no row with 199999.")
    }

  }

  it should "correctly find using 'in' syntax" in {
    val time = new Date()
    fixture.table.insert(999999, "strId", time, 45)
    fixture.table.insert(4, "strId", time, 45)
    val rows = fixture.table.filter(where("id") in Set(999999,3,4))
    assert(rows.size == 2)
    val rows2 = fixture.table.filter(where("id") in Set(999999,3))
    assert(rows2.head.id === 999999)
  }

  it should "correctly find using 'not in' syntax" in {
    val time = new Date()
    fixture.table.insert(999999, "strId", time, 45)
    fixture.table.insert(4, "strId", time, 45)
    val rows = fixture.table.filter(where("id") notIn Set(999999,3,4))
    assert(rows.size == 0)
    val rows2 = fixture.table.filter(where("id") notIn Set(3))
    assert(rows2.size === 2)
  }

  it should "correctly find using 'in' syntax with orderby and limit " in {
    val time = new Date()
    fixture.table.insert(999999, "strId", time, 45)
    fixture.table.insert(4, "strId", time, 45)
    val rows = fixture.table.filter(where("id") in Set(999999,3,4) orderBy OrderAsc("id") limit 1)
    assert(rows.head.id === 4)
  }

  it should "correctly find using existing where plus 'in' syntax with orderby and limit " in {
    val time = new Date()
    fixture.table.insert(999999, "strId", time, 45)
    fixture.table.insert(4, "strId", time, 45)
    val rows = fixture.table.filter(
      where("id > ?", 4) and
        where ("id") in Set(999999,3,4)
        orderBy OrderAsc("id")
        limit 1
    )
    assert(rows.head.id === 999999)
  }
}