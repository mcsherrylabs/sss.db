package sss.db

import org.scalatest._
import java.util.Date

trait DbV2Spec {

  self: FlatSpec with Matchers =>

  "A Db " should " allow persist(update) using a map " in {

    val time = new Date()
    val dbUnderTest = Db("testDb")
    val table = dbUnderTest.table("test")
    val r1 = table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 67)))
    val r2 = table.persist(Map(("id" -> r1("id")), ("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 68)))

    assert(r2[Int]("intVal") == 68)

    dbUnderTest.shutdown

  }

  it should " allow persist(insert) using a map with no id " in {

    val time = new Date()
    val dbUnderTest = Db("testDb")
    val table = dbUnderTest.table("test")
    val r1 = table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45)))

    println(s"$r1")
    val r2 = table.getRow(s"id = ${r1("id")}")
    assert(r1 == r2.get)

    dbUnderTest.shutdown

  }

  it should " allow dynamic table access " in {

    val time = new Date()
    val dbUnderTest = Db("testDb")
    val table = dbUnderTest.test
    val r1 = table.persist(Map(("strId" -> "strId"), ("createTime" -> time), ("intVal" -> 45)))

    println(s"$r1")
    val r2 = table.getRow(s"id = ${r1("id")}")
    assert(r1 == r2.get)

    dbUnderTest.shutdown
  }

}