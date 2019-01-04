package sss.db

import java.io.{DataInputStream, InputStream}
import java.util.Date

import org.scalatest.DoNotDiscover

import scala.collection.mutable

//@DoNotDiscover
class DbV2Spec extends DbSpecSetup {

  "A Db " should " allow persist(update) using a map " in {
    val time = new Date()
    val table = fixture.dbUnderTest.table("test")
    val r1 = fixture.table.persist(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 67))
    val r2 = fixture.table.persist(Map(("id" -> r1("id")), ("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 68)))
    assert(r2("intVal") === 68)
  }

  it should " allow a specific value even in an identity field " in {

    val time = new Date()
    val r1 = fixture.table.insert(Map(("id" -> 1000), ("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
    assert(r1("id") === 1000)
    val r2 = fixture.table.get(1000).get
    assert(r2 === r1)

  }
  it should " deal with '0' id in map " in {

    val time = new Date()
    val r1 = fixture.table.persist(Map(("id" -> 0), ("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
    assert(r1("id") !== 0)
    // save second Map
    val r2 = fixture.table.persist(Map(("id" -> 0), ("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 68)))
    assert(r2("intVal") === 68)
    assert(r2[Int]("intVal") !== r1[Int]("intVal"))

  }

  it should " allow persist(insert) using a map with no id " in {

    val time = new Date()
    val r1 = fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45)))
    val r2 = fixture.table.find(where(ps"id = ${r1("id")}"))
    assert(r1 == r2.get)

  }

  it should " allow persist(update) using a map with id " in {

    val time = new Date()
    val r1 = fixture.table.persist(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 45))
    val id = r1[Long]("id")
    val r2 = fixture.table.persist(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 90, "id" -> id))
    assert(r2[Int]("intVal") == 90)
    val r3 = fixture.table(id)
    assert(r3[Int]("intVal") == 90)

  }

  it should " allow dynamic table access " in {

    val time = new Date()
    val r1 = fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45)))

    val r2 = fixture.table.find(where("id = ?", r1("id")))
    assert(r1 == r2.get)
  }

  it should " count table rows " in {

    val time = new Date()
    for (i <- 0 to 10) fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45)))
    assert(11 == fixture.table.count)

  }

  it should " be able to delete a row" in {

    val time = new Date()
    val r1 = fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45)))
    val r2 = fixture.table.find(where("id = ?", r1("id")))
    assert(r1 == r2.get)
    val deletedRowCount = fixture.table.delete(where(ps"id = ${r1("id")}"))
    assert(deletedRowCount === 1)
    assert(fixture.table.find(where("id = ?", r1("id"))).isEmpty)
  }

  it should " be able to delete a row but respect limit " in {

    val time = new Date()
    val r1 = fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45)))
    val r3 = fixture.table.persist(Map(("strId" -> "strId2"), ("createTime" -> time), ("intVal" -> 45)))

    val rowFilter = where("id > ?", 0)

    val r2 = fixture.table.filter(rowFilter)
    assert(r2.size > 1, "Should be more than 1 to make the delete limit take effect")

    val deletedRowCountWithLimit = fixture.table.delete(rowFilter limit 1)
    assert(deletedRowCountWithLimit === 1)

    val deletedRowCount = fixture.table.delete(rowFilter)
    assert(deletedRowCount >= 1, "It should have deleted at least one other row once the limit is removed.")

  }

  it should " support paging in large tables " in {

    val time = new Date()
    for (i <- 0 to 100) {
      fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> i)))
    }

    for (i <- 0 to 10) {
      val page = fixture.table.page(i * 10, 10)
      (i * 10 to (i * 10) + 10) zip page foreach {
        case (a, b) =>
          assert(a == b[Int]("intVal"))
      }
    }

    val page = fixture.table.page(1000, 1)
    assert(page.isEmpty)
  }

  it should " support ordered paging in large tables " in {

    val time = new Date()
    for (i <- 0 to 100) {
      fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> i)))
    }

    for (i <- 10 to 0) {
      val page = fixture.table.page(i * 10, 10, Seq(OrderDesc("intVal")))
      (i * 10 to (i * 10) + 10) zip page foreach {
        case (a, b) =>
          assert(a == b[Int]("intVal"))
      }
    }

    val page = fixture.table.page(1000, 1)
    assert(page.isEmpty)
  }
  it should " support views  " in {

    val time = new Date()

    try {
      fixture.table.tx {

        fixture.dbUnderTest.createView("CREATE VIEW testview2 AS SELECT strId, intVal FROM test WHERE intVal > 50")
        for (i <- 0 to 100) {
          fixture.table.persist(Map("strId" -> "strId", "createTime" -> time, "intVal" -> i))
        }
        val view = fixture.dbUnderTest.view("testview2")
        assert(view.count == 50)
        val empty = view.filter(where("intVal < 50"))
        assert(empty.isEmpty)
      }
    } finally fixture.dbUnderTest.dropView("testview2")
  }

  it should " support optimistic locking using the version field " in {

    val time = new Date()

    val table = fixture.dbUnderTest.testVersion

    table.tx {
      val m = table.persist(Map(("version" -> 0), ("strId" -> "Hello there world")))
      val version = m[Long]("version")
      assert(version == 0)
      val m2 = table.persist(m)
      val version2 = m2[Long]("version")
      assert(version2 == version + 1)
    }
  }

  it should " throw exception if optimistic locking clash happens " in {

    val table = fixture.dbUnderTest.testVersion

    val m = table.tx {
      val m = table.persist(Map("version" -> 0, "strId" -> "Hello there world"))
      val version = m[Long]("version")
      assert(version == 0)
      //save row
      val m2 = table.persist(m)
      // now return 'stale' row
      m
    }
    intercept[DbOptimisticLockingException](table.persist(m))
  }

  it should " support persisting a byte as Binary" in {

    val testByte: Byte = 34
    val table = fixture.dbUnderTest.testBinary
    table.tx {
      val m = table.persist(Map("byteVal" -> testByte))
      assert(m[Byte]("byteVal") === testByte)
      val empty = table.persist(Map("byteVal" -> None))
      assert(Option(empty[Byte]("byteVal")).isEmpty)
      assert(empty[Option[Byte]]("byteVal") === None)
    }

  }

  it should " support persisting binary arrays as a blob " in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    table.tx {
      val m = table.persist(Map("blobVal" -> testStr.getBytes))
      assert(m[Array[Byte]]("blobVal") === testStr.getBytes)
      assert(new String(m[Array[Byte]]("blobVal")) === testStr)
    }

  }

  it should " support persisting wrapped binary arrays as a blob " in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    val wAry : mutable.WrappedArray[Byte] = testStr.getBytes
    table.tx {
      val m = table.persist(Map("blobVal" -> wAry))
      assert(m[mutable.WrappedArray[Byte]]("blobVal") === wAry)
      assert(new String(m[mutable.WrappedArray[Byte]]("blobVal").array) === testStr)
    }

  }
  it should " support find along binary arrays " in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    val bytes = testStr.getBytes
    table.persist(Map("blobVal" -> bytes))

    table.tx {
      val found = table.find(where("blobVal = ?", bytes))
      assert(found.isDefined)
      assert(found.get[Array[Byte]]("blobVal") === bytes)
      assert(new String(found.get[mutable.WrappedArray[Byte]]("blobVal").array) === testStr)
    }

  }

  it should " NOT support find along wrapped binary arrays (you must use .array)" in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    val wAry : mutable.WrappedArray[Byte] = testStr.getBytes
    val m = table.persist(Map("blobVal" -> wAry))

    table.tx {
      val found = table.find(where("blobVal = ?", wAry.array))
      assert(found.isDefined)
      assert(found.get[mutable.WrappedArray[Byte]]("blobVal") === wAry)
      assert(new String(found.get[mutable.WrappedArray[Byte]]("blobVal").array) === testStr)
    }

  }

  it should " support inputstream result " in {

    val testStr = "Hello My Friend"
    val table = fixture.dbUnderTest.testBinary
    val bytes = testStr.getBytes
    table.persist(Map("blobVal" -> bytes))

    table.tx {
      val found = table.find(where("blobVal = ?", bytes))
      assert(found.isDefined)
      val is = found.get[InputStream]("blobVal")
      val readBytes = new Array[Byte](bytes.length)
      val dataIs = new DataInputStream(is)
      dataIs.readFully(readBytes)
      assert(readBytes === bytes)
      assert(new String(readBytes) === testStr)
    }

  }

  it should " support use of boolean fields " in {

    val table = fixture.dbUnderTest.testBool
    val f = table.persist(Map("boolVal" -> false))
    val m = table.persist(Map("boolVal" -> true))
    val r = table.get(m("id"))
    assert(r.isDefined)
    assert(r.get("boolVal") === true)
    assert(r.get[Boolean]("boolVal") === true)
    val found = table.find(where("boolVal = ?", true))
    assert( found.isDefined)

  }

  it should " get the id of a row given criteria " in {

    val table = fixture.dbUnderTest.test
    val r = table.insert(Map("strId" -> "hellothere"))
    val theId = table.toLongId("strId" -> "hellothere")
    assert( theId === r[Long]("id"))
  }

  it should " get the ids of many rows given criteria " in {

    val table = fixture.dbUnderTest.test
    val rowsIds = ((0 to 10) map {_ => table.insert(Map("strId" -> "manyhellos"))}).map(_[Long]("id"))
    val theIds = table.toLongIds("strId" -> "manyhellos")
    assert(theIds.size === rowsIds.size)
    assert(theIds.filterNot(rowsIds.contains(_)).isEmpty)

  }

  it should " get the Int ids of many rows given criteria " in {

    val table = fixture.dbUnderTest.testIntId
    val rowsIds = ((0 to 10) map {_ => table.insert(Map("strVal" -> "manyhellos"))}).map(_[Int]("id"))
    val theIds = table.toIntIds("strVal" -> "manyhellos")

    assert(table.toIntIdOpt("strVal" -> "not there").isEmpty)

    assert(theIds.size === rowsIds.size)
    assert(theIds.filterNot(rowsIds.contains(_)).isEmpty)

  }

  it should " get the maximum value of a column " in {

    val table = fixture.dbUnderTest.testIntId
    val currentMax = table.maxId

    ((0 until 10) map {_ => table.insert(Map("strVal" -> "manyhellos"))})

    assert(table.maxId == currentMax + 10)

    table.delete(where("id > 0"))

    assert(table.maxId == 0)

    ((0 until 10) map {_ => table.insert(Map("strVal" -> "manyhellos"))})

    assert(table.count == 10)
    assert(table.maxId == currentMax + 20)

  }
}