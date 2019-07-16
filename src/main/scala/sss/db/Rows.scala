package sss.db


import java.sql.{Blob, ResultSet, ResultSetMetaData}
import java.sql.ResultSetMetaData.columnNoNulls
import java.util.Locale

import scala.annotation.tailrec


object Rows {
  def apply(rs: ResultSet, freeBlobsEarly: Boolean): Rows = parse(rs, freeBlobsEarly)

  def columnsMetaInfo(meta: ResultSetMetaData): ColumnsMetaInfo = {
    (1 to meta.getColumnCount).map { i =>
      ColumnMetaInfo(
        meta.getColumnName(i).toLowerCase(Locale.ROOT),
        meta.getColumnType(i),
        meta.isNullable(i) == columnNoNulls)
    }
  }

  def parse(rs: ResultSet,freeBlobsEarly: Boolean): Rows = {

    try {

      val meta = rs.getMetaData
      val colMax = meta.getColumnCount

      @tailrec
      def parse(rows: List[Row], rs: ResultSet): Rows = {
        if (rs.next) {
          var r = Map[String, Any]()
          for (i <- 1 to colMax) {

            val colName = meta.getColumnName(i).toLowerCase(Locale.ROOT)
            val o = rs.getObject(i)

            if (freeBlobsEarly && o.isInstanceOf[Blob]) {
              val jDBCBlobClient = o.asInstanceOf[Blob]
              try {
                val asByteAry = jDBCBlobClient.getBytes(1, jDBCBlobClient.length.toInt)
                r += (colName -> asByteAry)
              } finally jDBCBlobClient.free()
            } else {
              r += (colName -> o)
            }
          }

          parse(new Row(r) +: rows, rs)
        } else rows.reverse.toIndexedSeq
      }

      parse(List[Row](), rs)
    } finally rs.close

  }

}