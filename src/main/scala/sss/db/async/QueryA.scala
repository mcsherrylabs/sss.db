package sss.db.async

import java.io.InputStream
import java.sql.{Connection, PreparedStatement}
import java.util.Date

import javax.sql.DataSource
import org.joda.time.LocalDate
import sss.ancillary.Logging
import sss.db.{DbError, Row, Where}

import scala.collection.mutable
import scala.language.implicitConversions



/**
  *

  * @param ds
  * @param freeBlobsEarly
  *
  * Note on idomatic try/finally. 'Try' doesn't add much here as I want to close the resource and
  * propagate the *Exception* see List.head source for example. Anything I expect to happen and
  * can recover from is not by (my) definition an Exception!
  *
  */

object QueryA
  extends Logging {

  protected val id = "id"
  protected val version = "version"

  private[db] def mapToSql(value: Any): Any = {
    value match {
      case v: String => v //s"'${v}'"
      case v: Date => v.getTime
      case v: LocalDate => v.toDate.getTime
      case null => null
      case Some(x) => mapToSql(x)
      case None => null
      case v: Boolean => v
      case v: Int => v
      case v: Long => v
      case v: Double => v
      case v: Byte => Array(v)
      case v: Array[Byte] => v
      case v: mutable.WrappedArray[_] => v.array
      case v: scala.math.BigDecimal => v.bigDecimal
      case v: scala.math.BigInt => v.bigInteger
      case v: Float => v
      case v: Enumeration#Value => v.id
      case v: InputStream => v
      case v => DbError(s"Can't turn ${value} ${value.getClass} into sql value..")
    }
  }

  def prepareStatement(
                                    conn: Connection,
                                    sql: String,
                                    params: Seq[Any],
                                    flags: Option[Int] = None): PreparedStatement = {

    val ps = flags match {
      case None => conn.prepareStatement(s"${sql}")
      case Some(flgs) => conn.prepareStatement(s"${sql}", flgs)
    }
    for (i <- params.indices) ps.setObject(i + 1, mapToSql(params(i)))
    ps
  }

  private def tuplesToWhere(lookup:(String, Any)*): Where = {
    val asMap = lookup.toMap
    val sqls = asMap.keys.map {k => s"$k = ?"}
    val sql = sqls.mkString(" AND ")
    new Where(sql, asMap.values.toSeq)
  }

}
