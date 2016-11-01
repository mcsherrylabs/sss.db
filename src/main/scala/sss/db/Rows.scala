package sss.db


import java.sql.{Blob, ResultSet}

import scala.annotation.tailrec


object Rows {
  def apply(rs: ResultSet, freeBlobsEarly: Boolean): Rows = parse(rs, freeBlobsEarly)

  def parse(rs: ResultSet,freeBlobsEarly: Boolean): Rows = {

    try {

      val meta = rs.getMetaData
      val colmax = meta.getColumnCount

      @tailrec
      def parse(rows: Rows, rs: ResultSet): Rows = {
        if (rs.next) {
          var r = Map[String, Any]()
          for (i <- 0 until colmax) {
            val o = rs.getObject(i + 1); // Is SQL the first column is indexed
            if(freeBlobsEarly && o.isInstanceOf[Blob]) {
              val jDBCBlobClient = o.asInstanceOf[Blob]
              try {
                val asByteAry = jDBCBlobClient.getBytes(1, jDBCBlobClient.length.toInt)
                r = r + (meta.getColumnName(i + 1).toLowerCase -> asByteAry)
              } finally jDBCBlobClient.free()
            } else {
              r = r + (meta.getColumnName(i + 1).toLowerCase -> o)
            }
          }
          parse(rows :+ (r: Row), rs)
        } else rows
      }

      parse(IndexedSeq[Row](), rs)
    } finally rs.close

  }

}