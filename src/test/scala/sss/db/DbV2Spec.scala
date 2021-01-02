package sss.db

import java.util.Date

import org.scalatest.DoNotDiscover


@DoNotDiscover
class DbV2Spec extends DbSpecSetup {

  "A Db " should " allow persist(update) using a map " in {
    val time = new Date()
    val db = fixture.dbUnderTest
    import db.runContext.ds
    val table = db.table("test")


    val r1 = fixture.table.persist(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 67)).runSync.get
    val r2 = fixture.table.persist(Map("id" -> r1("id"), "strId" -> "strId", "createTime" -> time, "intVal" -> 68)).runSyncUnSafe
    assert(r2("intVal") === 68)
  }

  it should " allow a specific value even in an identity field " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()
    val r1 = fixture.table.insert(Map(("id" -> 1000), ("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67))).runSyncUnSafe
    assert(r1("id") === 1000)
    val r2 = fixture.table.get(1000).runSyncUnSafe.get
    assert(r2 === r1)

  }
  it should " deal with '0' id in map " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val time = new Date()
    val r1 = fixture.table.persist(Map(("id" -> 0), ("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67))).runSyncUnSafe
    assert(r1("id") !== 0)
    // save second Map
    val r2 = fixture.table.persist(Map(("id" -> 0), ("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 68))).runSyncUnSafe
    assert(r2("intVal") === 68)
    assert(r2[Int]("intVal") !== r1[Int]("intVal"))

  }

  it should " allow persist(insert) using a map with no id " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()
    val r1 = fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45))).runSyncUnSafe
    val r2 = fixture.table.find(where(ps"id = ${r1("id")}")).runSyncUnSafe
    assert(r1 == r2.get)

  }

  it should " allow persist(update) using a map with id " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()
    val r1 = fixture.table.persist(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 45)).runSyncUnSafe
    val id = r1[Long]("id")
    val r2 = fixture.table.persist(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 90, "id" -> id)).runSyncUnSafe
    assert(r2[Int]("intVal") == 90)
    val r3 = fixture.table(id).runSyncUnSafe
    assert(r3[Int]("intVal") == 90)

  }

  it should " allow dynamic table access " in {
    val db = fixture.dbUnderTest
    import db.runContext.ds
    val time = new Date()
    val r1 = fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45))).runSyncUnSafe

    val r2 = fixture.table.find(where("id = ?", r1("id"))).runSyncUnSafe
    assert(r1 == r2.get)
  }

  it should "count table rows " in {
    val db = fixture.dbUnderTest
    import db.runContext.ds
    val time = new Date()

    FutureTx.sequence((0 to 10) map { i =>
      fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45)))
    }).runSyncUnSafe

    assert(11 == fixture.table.count.runSyncUnSafe)

  }

  it should "respect null first and last" in {
    val db = fixture.dbUnderTest
    import db.runContext.ds
    val time = new Date()
    (for {
      _ <- fixture.table.persist(Map("strId" -> None, "createTime" -> time, "intVal" -> 101))
      _ <- fixture.table.persist(Map("strId" -> None, "createTime" -> time, "intVal" -> 102))
      _ <- fixture.table.persist(Map("strId" -> "103", "createTime" -> time, "intVal" -> 103))
      _ <- fixture.table.persist(Map("strId" -> "104", "createTime" -> time, "intVal" -> 104))
    } yield ()).runSyncUnSafe


    val nullsLastRows = fixture.table.filter(
      where("intVal > ?", 100)
        orderBy OrderAsc("strId", NullOrder.NullsLast)
    ).runSyncUnSafe

    nullsLastRows.drop(2).foreach(r => assert(r[Option[String]]("strId").isEmpty))
    nullsLastRows take (2) foreach (r => assert(r[Option[String]]("strId").isDefined))

    val nullsFirstRows = fixture.table.filter(
      where("intVal > ?", 100)
        orderBy OrderAsc("strId", NullOrder.NullsFirst)
    ).runSyncUnSafe

    nullsFirstRows drop (2) foreach (r => assert(r[Option[String]]("strId").isDefined))
    nullsFirstRows take (2) foreach (r => assert(r[Option[String]]("strId").isEmpty))
  }

  it should " be able to delete a row" in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val time = new Date()
    val r1 = fixture.table.persist(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 45)).runSyncUnSafe
    val r2 = fixture.table.find(where("id = ?", r1("id"))).runSyncUnSafe
    assert(r1 == r2.get)
    val deletedRowCount = fixture.table.delete(where(ps"id = ${r1("id")}")).runSyncUnSafe
    assert(deletedRowCount === 1)
    assert(fixture.table.find(where("id = ?", r1("id"))).runSyncUnSafe.isEmpty)
  }

  it should " be able to delete a row but respect limit " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()
    val r1 = fixture.table.persist(Map("strId" -> "strId", "createTime" -> time, "intVal" -> 45)).runSyncUnSafe
    val r3 = fixture.table.persist(Map("strId" -> "strId2", "createTime" -> time, "intVal" -> 45)).runSyncUnSafe

    val rowFilter = where("id > ?", 0)

    val r2 = fixture.table.filter(rowFilter).runSyncUnSafe
    assert(r2.size > 1, "Should be more than 1 to make the delete limit take effect")

    val deletedRowCountWithLimit = fixture.table.delete(rowFilter limit 1).runSyncUnSafe
    assert(deletedRowCountWithLimit === 1)

    val deletedRowCount = fixture.table.delete(rowFilter).runSyncUnSafe
    assert(deletedRowCount >= 1, "It should have deleted at least one other row once the limit is removed.")

  }

  it should " support paging in large tables " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()
    FutureTx.sequence((0 to 100) map { i =>
      fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> i)))
    }).runSyncUnSafe

    for (i <- 0 to 10) {
      val page = fixture.table.page(i * 10, 10).runSyncUnSafe
      (i * 10 to (i * 10) + 10) zip page foreach {
        case (a, b) =>
          assert(a == b[Int]("intVal"))
      }
    }

    val page = fixture.table.page(1000, 1).runSyncUnSafe
    assert(page.isEmpty)
  }

  it should " support ordered paging in large tables " in {
    val db = fixture.dbUnderTest
    import db.runContext.ds
    val time = new Date()

    FutureTx.sequence((0 to 100) map { i =>
      fixture.table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> i)))
    }).runSyncUnSafe

    for (i <- 10 to 0) {
      val page = fixture.table.page(i * 10, 10, Seq(OrderDesc("intVal"))).runSyncUnSafe
      (i * 10 to (i * 10) + 10) zip page foreach {
        case (a, b) =>
          assert(a == b[Int]("intVal"))
      }
    }

    val page = fixture.table.page(1000, 1).runSyncUnSafe
    assert(page.isEmpty)
  }
  it should "support views" in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()

    try {

      (for {
        _ <- fixture.dbUnderTest.createView("CREATE VIEW testview2 AS SELECT strId, intVal FROM test WHERE intVal > 50")
        _ <- FutureTx.sequence((0 to 100) map { i =>
          fixture.table.persist(Map("strId" -> "strId", "createTime" -> time, "intVal" -> i))
        })
      } yield ()).runSyncUnSafe


      val view = fixture.dbUnderTest.view("testview2")
      assert(view.count.runSyncUnSafe == 50)
      val empty = view.filter(where("intVal < 50")).runSyncUnSafe
      assert(empty.isEmpty)

    } finally fixture.dbUnderTest.dropView("testview2").runSyncUnSafe
  }

  it should " support optimistic locking using the version field " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()

    val table = fixture.dbUnderTest.table("testVersion")


    val (version1, version2) = (for {
      m <- table.persist(Map("version" -> 0, "strId" -> "Hello there world"))
      v1 = m[Long]("version")
      m2 <- table.persist(m)
      v2 = m2[Long]("version")
    } yield (v1, v2)).runSyncUnSafe


    assert(version1 == 0)
    assert(version2 == version1 + 1)

  }

  it should "throw exception if optimistic locking clash happens" in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val table = db.table("testVersion")

    val result = (for {
      m <- table.persist(Map("version" -> 0, "strId" -> "Hello there world"))
      version = m[Long]("version")
      m2 <- table.persist(m)
      err <- table.persist(m)
    } yield err).runSync

    assert(result.isFailure)

    result.recover {
      case e: DbOptimisticLockingException =>
      case e => fail(e)
    }
  }


  it should " support use of boolean fields " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val table = db.table("testBool")

    val f = table.persist(Map("boolVal" -> false)).runSyncUnSafe
    val m = table.persist(Map("boolVal" -> true)).runSyncUnSafe
    val r = table.get(m("id")).runSyncUnSafe
    assert(r.isDefined)
    assert(r.get("boolVal") === true)
    assert(r.get[Boolean]("boolVal") === true)
    val found = table.find(where("boolVal = ?", true)).runSyncUnSafe
    assert(found.isDefined)

  }

  it should " get the id of a row given criteria " in {
    val db = fixture.dbUnderTest
    import db.runContext.ds
    val table = db.table("test")

    val r = table.insert(Map("strId" -> "hellothere")).runSyncUnSafe
    val theId = table.toLongId("strId" -> "hellothere").runSyncUnSafe
    assert(theId === r[Long]("id"))
  }

  it should " get the ids of many rows given criteria " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val table = db.table("test")

    val rowsIds = FutureTx.sequence(
    (0 to 10) map (_ => table.insert(Map("strId" -> "manyhellos")) map (_.id))
    ).runSyncUnSafe


    val theIds = table.toLongIds("strId" -> "manyhellos").runSyncUnSafe
    assert(theIds.toSet === rowsIds.toSet)

  }

  it should " get the Int ids of many rows given criteria " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val table = db.table("testIntId")

    val rowsIds = FutureTx.sequence(
      (0 to 10) map (_ => table.insert(Map("strVal" -> "manyhellos")) map (_[Int]("id")))
    ).runSyncUnSafe

    val theIds = table.toIntIds("strVal" -> "manyhellos").runSyncUnSafe

    assert(table.toIntIdOpt("strVal" -> "not there").runSyncUnSafe.isEmpty)

    assert(theIds.toSet === rowsIds.toSet)

  }

  it should " get the maximum value of a column " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    val table = db.table("testIntId")
    val currentMax = table.maxId().runSyncUnSafe

    FutureTx.sequence(
    (0 until 10) map {_ => table.insert(Map("strVal" -> "manyhellos"))}
    ).runSyncUnSafe

    assert(table.maxId().runSyncUnSafe == currentMax + 10)

    table.delete(where("id > 0")).runSyncUnSafe

    assert(table.maxId().runSyncUnSafe == 0)

    FutureTx.sequence(
      (0 until 10) map {_ => table.insert(Map("strVal" -> "manyhellos"))}
    ).runSyncUnSafe


    assert(table.count.runSyncUnSafe == 10)
    assert(table.maxId().runSyncUnSafe == currentMax + 20)

  }

  it should "insert Enumeration Values" in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    object TestEnum extends Enumeration {
      type TestEnum = Value
      val Test1 = Value(4564345)
      val Test2 = Value(4564346)
    }

    fixture.table.persist(Map("strId" -> "strId", "intVal" -> TestEnum.Test1)).runSyncUnSafe
    fixture.table.persist(Map("strId" -> "strId", "intVal" -> TestEnum.Test2)).runSyncUnSafe

    val rows = fixture.table.filter(
      where("intVal") in Set(TestEnum.Test1, TestEnum.Test2)
    ).runSyncUnSafe

    assert(rows.size === 2)
    assert(rows.forall(r => Seq(TestEnum.Test1.id, TestEnum.Test2.id).contains(r[Int]("intVal"))))
  }
}
