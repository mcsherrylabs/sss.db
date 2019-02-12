package sss.db

import java.io.InputStream
import java.sql.PreparedStatement
import java.util.Date

import javax.sql.DataSource
import org.joda.time.LocalDate
import sss.ancillary.Logging
import sss.db.datasource.DataSource.CloseableDataSource

import scala.collection.generic.FilterMonadic
import scala.collection.mutable
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.runtime.universe._


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

class Query private[db] (private val selectSql: String,
                         implicit val ds: DataSource,
            freeBlobsEarly: Boolean)
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

  private[db] def prepareStatement(
                                    sql: String,
                                    params: Seq[Any],
                                    flags: Option[Int] = None): Transaction[PreparedStatement] = { context =>
    Future {
      val ps = flags match {
        case None => context.conn.prepareStatement(s"${sql}")
        case Some(flgs) => context.conn.prepareStatement(s"${sql}", flgs)
      }
      for (i <- params.indices) ps.setObject(i + 1, mapToSql(params(i)))
      ps
    }(context.ec)
  }

  private def tuplesToWhere(lookup:(String, Any)*): Where = {
    val asMap = lookup.toMap
    val sqls = asMap.keys.map {k => s"$k = ?"}
    val sql = sqls.mkString(" AND ")
    new Where(sql, asMap.values.toSeq)
  }

  /**
    *
    * @param sql
    * @return
    */
  def getRow(sql: Where): Transaction[Option[Row]] = {

    filter(sql) map { rows =>

      rows.size match {
        case 0 => None
        case 1 => Some(rows(0))
        case size => DbError(s"Should be 1 or 0, is -> ${size}")
      }
    }
  }


  def getRow(rowId: Long): Transaction[Option[Row]] = getRow(where(id -> rowId))

  def map[B, W <% Where](f: Row => B, where: W = where()): Transaction[QueryResults[B]] = filter(where).map(_ map f)

  def flatMap[B, W <% Where](f: Row => QueryResults[B], where: W = where()): Transaction[QueryResults[B]] = {
    map(identity, where)
      .map(
      _.foldLeft(List[B]())((acc,e) => acc ++ f(e))
      .toIndexedSeq)
  }

  def withFilter(f: Row => Boolean): Transaction[FilterMonadic[Row,QueryResults[Row]]] = map(identity).map(_.withFilter(f))

  def foreach[W <% Where](f: Row => Unit, where: W = where()): Transaction[QueryResults[Unit]] = map(f, where)

  /**
    * Note - You can put ORDER BY into the Where clause ...
    *
    * @param where
    * @return
    */
  def filter(where: Where): Transaction[Rows] = {

    prepareStatement(s"${selectSql} ${where.sql}", where.params) map { ps =>
      try {
        val rs = ps.executeQuery
        Rows(rs, freeBlobsEarly)
      } finally ps.close
    }
  }

  def filter(lookup: (String, Any)*): Transaction[Rows] = filter(tuplesToWhere(lookup: _*))

  def find(lookup: (String, Any)*): Transaction[Option[Row]] = find(tuplesToWhere(lookup: _*))

  def find(sql: Where): Transaction[Option[Row]] = getRow(sql)

  def apply(id: Long): Transaction[Row] = getRow(id).map(_.getOrElse(DbException(s"No row with id ${id}")))

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

  def toIdOpt[T >: Long with Int: TypeTag](lookup: (String, Any)*): Transaction[Option[T]] =
    find(lookup: _*).map(_.map(_[T](id)))

  def toId[T >: Long with Int: TypeTag](lookup: (String, Any)*): Transaction[T] =
    toIdOpt[T](lookup: _*).map(_.get)

  def toIds[T >: Long with Int: TypeTag](lookup: (String, Any)*): Transaction[Seq[T]] =
    filter(lookup: _*).map(_.map(_[T](id)))

  def toIntId(lookup: (String, Any)*): Transaction[Int] = toId[Int](lookup: _*)
  def toIntIdOpt(lookup: (String, Any)*): Transaction[Option[Int]] = toIdOpt[Int](lookup:_*)
  def toIntIds(lookup: (String, Any)*): Transaction[Seq[Int]] = toIds[Int](lookup:_*)

  def toLongId(lookup:(String, Any)*): Transaction[Long] = toId[Long](lookup:_*)
  def toLongIdOpt(lookup:(String, Any)*): Transaction[Option[Long]] = toIdOpt[Long](lookup:_*)
  def toLongIds(lookup:(String, Any)*): Transaction[Seq[Long]] = toIds[Long](lookup:_*)

  def get(id: Long): Transaction[Option[Row]] = getRow(id)

  def page(start: Long, pageSize: Int, orderClauses: Seq[OrderBy] = Seq(OrderAsc("id"))): Transaction[Rows] = { context =>
    Future {
      val st = context.conn.createStatement()
      try {
        val clause = where() orderBy (orderClauses: _*) limit(start, pageSize)
        val rs = st.executeQuery(s"${selectSql} ${clause.sql}")
        Rows(rs, freeBlobsEarly)
      } finally st.close()
    }(context.ec)
  }

  def toPaged(pageSize: Int,
              filter: Where = where(),
              indexCol: String = "id") =
    PagedView(this, pageSize, filter, indexCol)

}
