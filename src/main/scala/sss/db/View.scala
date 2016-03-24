package sss.db

import java.sql.{Connection, PreparedStatement}
import java.util.Date
import javax.sql.DataSource

import org.joda.time.LocalDate
import sss.ancillary.Logging

import scala.collection.mutable

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
      val clause = if(orderClauses.nonEmpty)
        " ORDER BY " + orderClauses.map {
          case OrderDesc(col) => s"$col DESC"
          case OrderAsc(col) => s"$col ASC"
        }.mkString(",")
        else ""

      val rs = st.executeQuery(selectSql + clause) // run the query
      Rows(rs).map(f)
    } finally {
      st.close()
    }
  }

  def filter(where: Where): Rows = tx {

    val ps = conn.prepareStatement(s"${selectSql} WHERE ${where.clause}"); // run the query
    try {

      for(i <- where.params.indices) ps.setObject(i + 1, where.params(i))
      val rs = ps.executeQuery
      Rows(rs)
    } finally {
      ps.close
    }
  }

  def find(sql: Where): Option[Row] = inTransaction[Option[Row]](getRow(sql))

  def apply(id: Long): Row = getRow(id).getOrElse(DbException(s"No row with id ${id}"))

  def get(id: Long): Option[Row] = tx[Option[Row]](getRow(id))

  def page(start: Int, pageSize: Int): Rows = tx {
    val st = conn.createStatement(); // statement objects can be reused with
    try {
      val rs = st.executeQuery(s"${selectSql} LIMIT ${start}, ${pageSize}")
      Rows(rs)
    } finally {
      st.close
    }
  }

  def count: Long = tx {
    val st = conn.createStatement(); // statement objects can be reused with
    try {
      val rs = st.executeQuery(s"SELECT COUNT(*) AS total FROM ${name}")
      if (rs.next) rs.getLong("total")
      else DbError("Database did not return count for table: $name")
    } finally {
      st.close
    }
  }

}
