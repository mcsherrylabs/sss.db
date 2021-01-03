package sss.db

import java.util.Date

import org.scalatest.DoNotDiscover
import sss.db.WhereOps.toWhere
import scala.util.control.NonFatal

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

  it should " be able to read all rows from a table (in order!) " in {


    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()

    val insertedT: FutureTx[QueryResults[Int]] = FutureTx.sequence(
      (0 to 10).map (fixture.table.insert(0, "strId", time, _))
    ) flatMap {  seqInts: Seq[Int] =>
      fixture.table.map ( r => r[Int]("intVal")
      , OrderDesc ("intVal"))
    }
    assert(insertedT.runSync.get === (0 to 10).reverse)

    val got = fixture.table.map (r => {
      r
    }).runSync.get

    assert(fixture.table.map (r => {
      r[Int]("intVal")
    }, OrderAsc("intVal")).runSync.get == (0 to 10))

  }

  it should " be able to find the row inserted " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()
    fixture.table.insert(0, "strId", time, 45).runSync
    val rows = fixture.table.filter(where(ps"createTime = ${time.getTime}")).runSync.get
    assert(rows.size === 1, "Should only be one row found !")
    val row = rows(0)
    assert(row("strId") === "strId")
    assert(row("createTime") === time.getTime)
    assert(row("intVal") === 45)
  }

  it should " support shorthand filter" in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()
    fixture.table.insert(3456, "strId", time, 45).runSync
    val rows = fixture.table.filter("createTime" -> time).runSync.get
    assert(rows.size === 1, "Should only be one row found !")
    val row = rows(0)
    assert(row("strId") === "strId")
    assert(row("createTime") === time.getTime)
    assert(row("intVal") === 45)
  }

  it should " be able to find the row inserted by id " in {
    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()
    (fixture.table.insert(99, "strId", time, 45) flatMap { i =>
      fixture.table.get(99) map {
        case None => fail("oh oh, failed to find row by id")
        case Some(r) => assert(r("id") === 99)
      }
    }).runSync.get
  }

  it should " be able to find the row searching by field name " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()

    val p = for {
      _ <- fixture.table.insert(99, "strId", time, 45)
      r <- fixture.table.find(where(s"id = ?", 99))
    } yield r match {
      case None => fail("oh oh, failed to find row by id")
      case Some(r1) => assert(r1("id") === 99)
    }

    p.runSync.get
  }

  it should " support shorthand find " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()
    val p = for {
      _ <- fixture.table.insert(100, "strId", time, 45)
      rOpt <- fixture.table.find("id" -> 100)
    } yield rOpt match {
      case None => fail("oh oh, failed to find row by id")
      case Some(r) => assert(r("id") === 100)
    }

    p.runSync.get
  }

  it should " not be able to find a single row when 2 are present " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val time = new Date()

    (for {
      _ <- fixture.table.insert(99, "strId", time, 45)
      _ <- fixture.table.insert(100, "strId", time, 45)
    } yield ()).runSync.get

    try {
      fixture.table.find(where(s"strId = ?", "strId")).runSync.get
      fail("there are 2 rows with strId,  should throw ...")
    } catch {
      case NonFatal(e)  =>
    }
  }

  it should "support a transaction" in {
    val time = new Date()
    val db = fixture.dbUnderTest
    import db.runContext.ds

    val p = for {
      _ <- fixture.table.insert(999999, "strId", time, 45)
      _ = throw new RuntimeException("Ah HA!")
      r <- fixture.table.insert(199999, "strId", time, 45)
    } yield r

    assert(p.runSync.isFailure)
    assert(fixture.table.find(idCol -> 199999).runSync.get.isEmpty)
    assert(fixture.table.find(idCol -> 999999).runSync.get.isEmpty)
  }

  it should "correctly find using 'in' syntax" in {
    val time = new Date()
    val db = fixture.dbUnderTest
    import db.runContext.ds

    val plan = for {
      _ <- fixture.table.insert(999999, "strId", time, 45)
      _ <- fixture.table.insert(4, "strId", time, 45)
      rows <- fixture.table.filter(where("id") in Set(999999,3,4))
      rows2 <- fixture.table.filter(where("id") in Set(999999,3))
    } yield (rows, rows2)

    val (rows, rows2) = plan.runSync.get

    assert(rows.size == 2)
    assert(rows2.head.id === 999999)
  }

  it should "correctly find using 'not in' syntax" in {
    val time = new Date()
    val db = fixture.dbUnderTest
    import db.runContext.ds

    val plan = for {
      _ <- fixture.table.insert(999999, "strId", time, 45)
      _ <-  fixture.table.insert(4, "strId", time, 45)
      rows <- fixture.table.filter(where("id") notIn Set(999999, 3, 4))
      _ = assert (rows.isEmpty)
      rows2 <- fixture.table.filter(where("id") notIn Set(3))
      _ = assert (rows2.size === 2)
    } yield ()

    plan.runSync.get
  }

  it should "correctly find using 'in' syntax with orderby and limit " in {
    val time = new Date()
    val db = fixture.dbUnderTest
    import db.runContext.ds

    val plan = for {
      _ <- fixture.table.insert(999999, "strId", time, 45)
      _ <- fixture.table.insert(4, "strId", time, 45)
      rows <- fixture.table.filter(where("id") in Set(999999, 3, 4) orderBy OrderAsc("id") limit 1)
      _ = assert(rows.head.id === 4)
    } yield ()

    plan.runSync.get
  }


  it should "correctly find using existing where plus 'in' syntax with orderby and limit " in {
    val time = new Date()
    val db = fixture.dbUnderTest
    import db.runContext.ds

    val plan = for {
      _ <- fixture.table.insert(999999, "strId", time, 45)
      _ <- fixture.table.insert(4, "strId", time, 45)
      rows <- fixture.table.filter(
        where("id > ?", 4) and
          where("id") in Set(999999, 3, 4)
          orderBy OrderAsc("id")
          limit 1
      )
    } yield rows

    val rows = plan.runSync.get
    assert(rows.head.id === 999999)
  }


  it should "correctly filter using None syntax " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds

    val table = fixture.dbUnderTest.table("testBinary")

    val plan = for {
      _ <- table.insert(Map("byteVal" -> None))
      //val all = table.map(identity)
      rows <- table.filter(where("byteVal" -> None))
      rowsWorks <- table.filter(where("byteVal IS NULL"))
      _ = assert(rowsWorks.nonEmpty)
      _ = assert(rows.nonEmpty)
      _ = assert(rowsWorks == rows)
    } yield ()

    plan.runSync.get

  }

  it should "correctly filter using is Not Null syntax " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds


    val table = fixture.dbUnderTest.table("testBinary")
    import IsNull._
    val plan = for {
      _ <- table.insert(Map("byteVal" -> Some(3.toByte)))
      //val all = table.map(identity)

      rows <- table.filter(where("byteVal") is NotNull)
      rowsWorks <- table.filter(where("byteVal IS NOT NULL"))
      _ = assert(rowsWorks.nonEmpty)
      _ = assert(rows.nonEmpty)
      _ = assert(rowsWorks == rows)
    } yield ()

  plan.runSync.get
  }

  it should "correctly filter using is Null syntax " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    import IsNull._

    val table = fixture.dbUnderTest.table("testBinary")

    val plan = for {
      _ <- table.insert(Map("byteVal" -> None))
      rows <- table.filter(where("byteVal") is Null)
      rowsWorks <- table.filter(where("byteVal IS NULL"))
      _ = assert(rowsWorks.nonEmpty)
      _ = assert(rows.nonEmpty)
      _ = assert(rowsWorks == rows)
    } yield ()


    plan.runSync.get


  }

}