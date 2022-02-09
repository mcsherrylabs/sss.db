package sss.db


import sss.ancillary.Logging

import scala.language.implicitConversions

/**
  *
  * @param name
  * @param runContext
  * @param freeBlobsEarly
  *
  * Note on idomatic try/finally. 'Try' doesn't add much here as I want to close the resource and
  * propagate the *Exception* see List.head source for example. Anything I expect to happen and
  * can recover from is not by (my) definition an Exception!
  *
  */
class View private[db] (val name: String,
                        runContext: RunContext,
                        freeBlobsEarly: Boolean,
                        columns: String = "*")
  extends Query(s"SELECT ${columns} from ${name}", runContext, freeBlobsEarly)  with Logging {


  def maxId(): FutureTx[Long] = max(id)

  def max(colName: String): FutureTx[Long] = { context =>
    LoggingFuture {
      val st = context.conn.createStatement() // statement objects can be reused with
      try {
        val rs = st.executeQuery(s"SELECT MAX($colName) AS max_val FROM ${name}")
        if (rs.next) {
          rs.getLong("max_val")
        } else DbError(s"Database did not return max($colName) for table: $name")
      } finally st.close()

    }(context.ec)
  }


  def count: FutureTx[Long] = { context =>
    LoggingFuture {
      val st = context.conn.createStatement() // statement objects can be reused with
      try {
        val rs = st.executeQuery(s"SELECT COUNT(*) AS total FROM ${name}")
        if (rs.next) rs.getLong("total")
        else DbError(s"Database did not return count for table: $name")
      } finally st.close()
    }(context.ec)
  }

}
