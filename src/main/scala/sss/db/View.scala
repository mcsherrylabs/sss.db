package sss.db

import java.sql.{Connection, PreparedStatement}
import java.util.Date
import javax.sql.DataSource

import org.joda.time.LocalDate
import sss.ancillary.Logging

import scala.collection.mutable
import scala.reflect.runtime.universe._


class View(val name: String, private[db] val ds: DataSource) extends Tx with Logging {

  protected val id = "id"
  protected val version = "version"

  private val selectSql = s"SELECT * from ${name}"
  private[db] def conn: Connection = Tx.get.conn

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
      case v: Array[Byte] => v
      case v: mutable.WrappedArray[_] => v.array
      case v: scala.math.BigDecimal => v.bigDecimal
      case v: scala.math.BigInt => v.bigInteger
      case v: Float => v
      case v => DbError(s"Can't turn ${value} ${value.getClass} into sql value..")
    }
  }

  private[db] def prepareStatement(sql: String, params: Seq[Any], flags: Option[Int] = None): PreparedStatement = {
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
    Where(sql, asMap.values.toSeq :_*)
  }

  private def orderByClausesToString(orderClauses: Seq[OrderBy]): String = {
    if(orderClauses.nonEmpty)
      " ORDER BY " + orderClauses.map {
        case OrderDesc(col) => s"$col DESC"
        case OrderAsc(col) => s"$col ASC"
      }.mkString(",")
    else ""
  }

  def getRow(sql: Where): Option[Row] = tx {
    val rows = filter(sql)

    rows.size match {
      case 0 => None
      case 1 => Some(rows(0))
      case size => throw new Error(s"Should be 1 or 0, is -> ${size}")
    }
  }

  def getRow(id: Long): Option[Row] = getRow(Where("id = ?", id))

  def map[B](f: Row => B, orderClauses: OrderBy*): IndexedSeq[B] = tx {

    val st = conn.createStatement(); // statement objects can be reused with
    try {
      val clause = orderByClausesToString(orderClauses)
      val rs = st.executeQuery(selectSql + clause) // run the query
      Rows(rs).map(f)
    } finally {
      st.close()
    }
  }

  /**
    * Note - You can put ORDER BY into the Where clause ...
    *
    * @param where
    * @return
    */
  def filter(where: Where): Rows = tx {

    val ps = prepareStatement(s"${selectSql} WHERE ${where.clause}", where.params) // run the query
    try {
      val rs = ps.executeQuery
      Rows(rs)
    } finally {
      ps.close
    }
  }

  def filter(lookup: (String, Any)*): Seq[Row] = filter(tuplesToWhere(lookup: _*))

  def find(lookup: (String, Any)*): Option[Row] = find(tuplesToWhere(lookup: _*))

  def find(sql: Where): Option[Row] = getRow(sql)

  def apply(id: Long): Row = getRow(id).getOrElse(DbException(s"No row with id ${id}"))

  /**
    * Shorthand way of getting the id from a row
    * Often the id is used as a fk but the lookup
    * key is a string, use this to get the id from
    * the lookup string.
    *
    * @param lookup (e.g. ("name" -> "John")
    * @tparam T
    * @return
    */

  def toIdOpt[T >: Long with Int: TypeTag](lookup: (String, Any)*): Option[T] = find(lookup: _*) map(_[T](id))

  def toId[T >: Long with Int: TypeTag](lookup: (String, Any)*): T = toIdOpt[T](lookup: _*).get
  def toIds[T >: Long with Int: TypeTag](lookup: (String, Any)*): Seq[T] = filter(lookup: _*) map(_[T](id))

  def toIntId(lookup: (String, Any)*): Int = toId[Int](lookup: _*)
  def toIntIdOpt(lookup: (String, Any)*): Option[Int] = toIdOpt[Int](lookup:_*)
  def toIntIds(lookup: (String, Any)*): Seq[Int] = toIds[Int](lookup:_*)

  def toLongId(lookup:(String, Any)*): Long = toId[Long](lookup:_*)
  def toLongIdOpt(lookup:(String, Any)*): Option[Long] = toIdOpt[Long](lookup:_*)
  def toLongIds(lookup:(String, Any)*): Seq[Long] = toIds[Long](lookup:_*)

  def get(id: Long): Option[Row] = getRow(id)

  def page(start: Long, pageSize: Int, orderClauses: Seq[OrderBy] = Seq(OrderAsc("id"))): Rows = tx {
    val st = conn.createStatement(); // statement objects can be reused with
    try {
      val orderClausesStr = orderByClausesToString(orderClauses)
      val rs = st.executeQuery(s"${selectSql} $orderClausesStr LIMIT ${start}, ${pageSize}")
      Rows(rs)
    } finally {
      st.close
    }
  }

  def maxId: Long = max(id)

  def max(colName: String): Long = tx {
    val st = conn.createStatement(); // statement objects can be reused with
    try {
      val rs = st.executeQuery(s"SELECT MAX($colName) AS max_val FROM ${name}")
      if (rs.next) {
        rs.getLong("max_val")
      } else DbError(s"Database did not return max($colName) for table: $name")
    } finally {
      st.close
    }
  }


  def count: Long = tx {
    val st = conn.createStatement(); // statement objects can be reused with
    try {
      val rs = st.executeQuery(s"SELECT COUNT(*) AS total FROM ${name}")
      if (rs.next) rs.getLong("total")
      else DbError(s"Database did not return count for table: $name")
    } finally {
      st.close
    }
  }

}
