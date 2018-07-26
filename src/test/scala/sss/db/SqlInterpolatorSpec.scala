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

  "OrderBys " should "respect the limit on copy " in {
    val w = where().limit(10)
    val w1 = w.orderBy(OrderDesc("col"))
    assert(w.orderBys.limit === w1.orderBys.limit)
  }

  "Where " should "allow applied params to change" in {
    val w = where("col" -> 21, "col2" -> 78)
    assert(w.params === Seq(21, 78))
    assert(w(34).params === Seq(34))
  }

}