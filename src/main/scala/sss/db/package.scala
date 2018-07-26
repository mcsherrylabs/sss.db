package sss

import java.io.{ByteArrayInputStream, InputStream}
import java.math.BigDecimal
import java.sql.Blob

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.language.implicitConversions
/**
 * @author alan
 */
package object db {

  object DbException { def apply(msg: String) = throw new DbException(msg) }
  object DbError { def apply(msg: String) = throw new DbError(msg) }

  class DbOptimisticLockingException(msg: String) extends RuntimeException(msg)
  class DbException(msg: String) extends RuntimeException(msg)
  class DbError(msg: String) extends Error(msg)

  type QueryResults[A] = IndexedSeq[A]
  type Rows = QueryResults[Row]


  type SimpleColumnTypes =
      String with
      Long with
      Short with
      Integer with
      Int with
      Float with
      Boolean with
      BigDecimal with
      Byte with
      Double with
      collection.mutable.WrappedArray[Byte] with
      Array[Byte] with
      java.sql.Date with
      java.sql.Time with
      java.sql.Timestamp with
      java.sql.Clob with
      java.sql.Blob with
      java.sql.Array with
      java.sql.Ref with
      java.sql.Struct with
      InputStream

  type ColumnTypes = SimpleColumnTypes with Option[SimpleColumnTypes]


  implicit class SqlHelper(val sc: StringContext) extends AnyVal {
    def ps(args: Any*): (String, Seq[Any]) = {
      (sc.parts.mkString("?"), args)
    }
  }

  implicit def toMap(r: Row): Map[String, _] = r.asMap

  type Limit = Option[Int]

  trait OrderBy

  object OrderBys {
    def apply(pageSize:Int, orderBys: OrderBy*): OrderBys = OrderBys(orderBys.toSeq, Some(pageSize))
    def apply(orderBys: OrderBy*): OrderBys = OrderBys(orderBys.toSeq, None)
  }

  sealed case class OrderBys(orderBys: Seq[OrderBy] = Seq.empty, limit: Limit = None) {
    def limit(limit: Int): OrderBys = OrderBys(orderBys, Some(limit))
    private [db] def sql: String = {
      orderByClausesToString(orderBys) + (limit match {
        case Some(l) => s" LIMIT $l"
        case None    => ""
      })
    }

    private def orderByClausesToString(orderClauses: Seq[OrderBy]): String = {
      if(orderClauses.nonEmpty)
        " ORDER BY " + orderClauses.map {
          case OrderDesc(col) => s"$col DESC"
          case OrderAsc(col) => s"$col ASC"
        }.mkString(",")
      else ""
    }
  }
  sealed case class OrderDesc(colName: String) extends OrderBy
  sealed case class OrderAsc(colName: String) extends OrderBy

  private[db] sealed class Where(
                                     private[db] val clause: String,
                                     private[db] val params: Seq[Any] = Seq.empty,
                                     private[db] val orderBys: OrderBys = OrderBys()) {
    private def copy(clause: String = this.clause,
                     params:Seq[Any] = this.params,
                     orderBys:OrderBys = this.orderBys): Where =
      new Where(clause, params , orderBys)

    def apply(prms: Any*): Where = copy(params = prms)
    def using(prms: Any*): Where = apply(prms :_*)
    def orderBy(orderBys: OrderBys): Where = copy(orderBys = orderBys)
    def orderBy(orderBys: OrderBy*): Where = copy(orderBys = OrderBys(orderBys, this.orderBys.limit))
    def limit(page: Int): Where = copy(orderBys = orderBys.limit(page))
    private [db] def sql: String = {
      val where = if(!clause.isEmpty) s" WHERE $clause" else ""
      where + orderBys.sql
    }
  }

  implicit def toWhere(orderBy: OrderBy): Where = new Where("", Seq.empty, OrderBys(orderBy))

  def where(): Where = new Where("")
  def where(sqlParams: (String, Seq[Any])): Where = new Where(sqlParams._1, sqlParams._2)
  def where(sql: String, params: Any*): Where = new Where(sql, params.toSeq)
  def where(tuples: (String, Any)*): Where = where (
    tuples.foldLeft[(String, Seq[Any])](("", Seq()))((acc, e) =>
      if (acc._1.isEmpty) (s"${e._1} = ?", acc._2 :+ e._2)
      else (s"${acc._1}  AND ${e._1} = ?", acc._2 :+ e._2)
      )
  )

  import scala.reflect.runtime.universe._

  implicit class Row(val asMap: Map[String, _]) {

    override def equals(o: Any) = o match {
      case that: Row => that.asMap.equals(asMap)
      case x => false
    }

    override def hashCode = asMap.hashCode

    def get(col: String) = asMap(col.toLowerCase)

    def apply[T >: ColumnTypes: TypeTag](col: String): T = {

      val rawVal = asMap(col.toLowerCase)
      val massaged = if (typeOf[T] <:< typeOf[Option[_]] && rawVal == null) {
        None
      } else if (typeOf[T] == typeOf[Array[Byte]] && rawVal.isInstanceOf[Blob]) {
        blobToBytes(rawVal.asInstanceOf[Blob])
      } else if (typeOf[T] == typeOf[mutable.WrappedArray[Byte]] && rawVal.isInstanceOf[Blob]) {
        blobToWrappedBytes(rawVal.asInstanceOf[Blob])
      } else if (typeOf[T] == typeOf[mutable.WrappedArray[Byte]] && rawVal.isInstanceOf[Array[Byte]]) {
        new WrappedArray.ofByte(rawVal.asInstanceOf[Array[Byte]])
      } else if (typeOf[T] == typeOf[InputStream] && rawVal.isInstanceOf[Blob]) {
        blobToStream(rawVal.asInstanceOf[Blob])
      } else if (typeOf[T] == typeOf[Byte] && rawVal.isInstanceOf[Array[Byte]]) {
        val aryByte = rawVal.asInstanceOf[Array[Byte]]
        assert(aryByte.length == 1)
        aryByte(0)
      } else if (typeOf[T] == typeOf[InputStream] && rawVal.isInstanceOf[Array[Byte]]) {
        val aryByte = rawVal.asInstanceOf[Array[Byte]]
        new ByteArrayInputStream(aryByte)
      } else if (typeOf[T] == typeOf[Option[_]])
        Some(rawVal)
      else rawVal

      massaged.asInstanceOf[T]
    }

    private def blobToStream(jDBCBlobClient: Blob): InputStream = jDBCBlobClient.getBinaryStream
    private def blobToBytes(jDBCBlobClient: Blob): Array[Byte]= jDBCBlobClient.getBytes(1, jDBCBlobClient.length.toInt)
    private def blobToWrappedBytes(jDBCBlobClient: Blob): mutable.WrappedArray[Byte]= jDBCBlobClient.getBytes(1, jDBCBlobClient.length.toInt)

    override def toString: String = {
      asMap.foldLeft("") { case (a, (k, v)) => a + s" Key:${k}, Value: ${v}" }
    }
  }
}