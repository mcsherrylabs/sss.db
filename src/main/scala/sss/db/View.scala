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
                        baseWhere: Where,
                        runContext: RunContext,
                        freeBlobsEarly: Boolean,
                        columns: String = "*")
  extends Query(s"SELECT ${columns} from ${name}", baseWhere, runContext, freeBlobsEarly)  with Logging {


  def maxId(): FutureTx[Long] = max(id)

  def max(colName: String): FutureTx[Long] =
    prepareStatement(s"SELECT MAX($colName) AS max_val FROM $name ${baseWhere.sql}", baseWhere.params) map { ps =>
      try {
        val rs = ps.executeQuery
        if (rs.next) {
          rs.getLong("max_val")
        } else DbError(s"Database did not return max($colName) for table: $name")
      } finally ps.close()
    }

  def count: FutureTx[Long] =
    prepareStatement(s"SELECT COUNT(*) AS total FROM $name ${baseWhere.sql}", baseWhere.params) map { ps =>
      try {
        val rs = ps.executeQuery()
        if (rs.next) rs.getLong("total")
        else DbError(s"Database did not return count for table: $name")
      } finally ps.close()
    }

}
