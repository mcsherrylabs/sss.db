package sss.db

import java.util.Date

import org.scalatest._

@DoNotDiscover
class SqlInterpolatorSpec extends FlatSpec with Matchers with BeforeAndAfter {

  it should " interpolate the string replacing the params with ? for prepared statements." in {
    val b = 56
    val c = new Date()

    val (goodSql, params)  = ps"SELECT all from x where a = $b AND b = ${c}"
    assert(goodSql === "SELECT all from x where a = ? AND b = ?")
    assert(params === Seq(56, c))

  }

  it should " not be used when column names are embedded in the string " in {
    val b = 56
    val c = new Date()
    val xCol = "someCol"

    val wher  = where(s"$xCol = ? AND b = ?") using (b, c)
    assert(wher.clause === "someCol = ? AND b = ?")
    assert(wher.params === Seq(56, c))

  }

  "OrderBy" should "not allow bad column names" in {
    OrderAsc("good")
    OrderDesc("also_good")
    intercept[IllegalArgumentException] {
      OrderDesc("3startwithnumber")
    }
    intercept[IllegalArgumentException] {
      OrderDesc("has space")
    }
    intercept[IllegalArgumentException] {
      OrderAsc("")
    }
    intercept[IllegalArgumentException] {
      OrderAsc("strangechar^")
    }
  }


  "OrderBys " should "respect the limit on copy " in {
    val w = where().limit(10)
    val w1 = w.orderBy(OrderDesc("col"))
    assert(w.orderBys.limit === w1.orderBys.limit)
  }

  "orderDesc created with string columns" should "produce descending cols " in {
    val w = where().orderDesc("a", "b", "c")
    assert(w.sql === " ORDER BY a DESC,b DESC,c DESC")
  }

  "orderDesc created with string columns" should "produce descending cols and respect limit " in {
    val w = where() orderDesc("a", "b", "c") limit 10
    assert(w.sql === " ORDER BY a DESC,b DESC,c DESC LIMIT 10")
  }

  "orderAsc created with string columns" should "produce ascending cols and respect limit" in {
    val w = where() orderAsc("a", "b", "c") limit 6
    assert(w.sql === " ORDER BY a ASC,b ASC,c ASC LIMIT 6")
  }

  "Where " should "allow applied params to change" in {
    val w = where("col" -> 21, "col2" -> 78)
    assert(w.params === Seq(21, 78))
    assert(w(34).params === Seq(34))
  }


  "in" should "produce correct sql " in {
    val w = where("col") in Set(1,2,3)
    assert(w.sql == " WHERE col IN (?,?,?)" )
  }

  "notIn" should "produce correct sql " in {
    val w = where("col") notIn Set(1,2,3)
    assert(w.sql == " WHERE col NOT IN (?,?,?)" )
  }

  "and" should "produce correct sql combining and's " in {
    val w = where("col") notIn Set(1,2,3) and where("id" -> 45 ) limit 3
    assert(w.sql == " WHERE col NOT IN (?,?,?) AND id = ? LIMIT 3" )
    assert(w.params == Seq(1,2,3, 45))
  }

  "and" should "produce correct sql for 'and' with blank where" in {
    val w = where() and where("id" -> 45 ) limit 3
    assert(w.sql == " WHERE id = ? LIMIT 3" )
    assert(w.params == Seq(45))
  }

}