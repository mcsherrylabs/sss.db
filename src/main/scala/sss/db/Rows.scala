package sss.db

import java.sql.ResultSet

import scala.annotation.tailrec

object Rows {
  def apply(rs: ResultSet): Rows = parse(rs)

  def parse(rs: ResultSet): Rows = {

    try {

      val meta = rs.getMetaData
      val colmax = meta.getColumnCount

      @tailrec
      def parse(rows: Rows, rs: ResultSet): Rows = {
        if (rs.next) {
          var r = Map[String, Any]()
          for (i <- 0 until colmax) {
            val o = rs.getObject(i + 1); // Is SQL the first column is indexed
            r = r + (meta.getColumnName(i + 1).toLowerCase -> o)
          }
          parse(rows :+ (r: Row), rs)
        } else rows
      }

      parse(IndexedSeq[Row](), rs)
    } finally rs.close

  }

}