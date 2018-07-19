package sss.db

import java.util.Date

import org.scalatest._

class SqlInterpolatorSpec extends FlatSpec with Matchers with BeforeAndAfter {

  it should " interpolate the string replacing the params with ? for prepared statements." in {
    val b = 56
    val c = new Date()

    val (goodSql, params)  = ps"SELECT * from x where a = $b AND b = ${c}"
    assert(goodSql == "SELECT * from x where a = ? AND b = ?")
    assert(params == Seq(56, c))

  }
}