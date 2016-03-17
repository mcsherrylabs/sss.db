package sss

import java.io.InputStream
import java.math.BigDecimal

import org.hsqldb.jdbc.JDBCBlobClient

import scala.language.implicitConversions
/**
 * @author alan
 */
package object db {

  /*implicit def flattenToMap(cc: AnyRef): Map[String, Any] =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc))
    }*/

  object DbException { def apply(msg: String) = throw new DbException(msg) }
  object DbError { def apply(msg: String) = throw new DbError(msg) }

  class DbOptimisticLockingException(msg: String) extends RuntimeException(msg)
  class DbException(msg: String) extends RuntimeException(msg)
  class DbError(msg: String) extends Error(msg)

  type Rows = IndexedSeq[Row]

  type ColumnTypes = String with Long with Short with Integer with Int with Float with Boolean with BigDecimal with Byte with Double with Array[Byte] with java.sql.Date with java.sql.Time with java.sql.Timestamp with java.sql.Clob with java.sql.Blob with java.sql.Array with java.sql.Ref with java.sql.Struct

  implicit def toMap(r: Row): Map[String, _] = r.asMap

  sealed case class Where(clause: String, params: Any*) {
    private[db] def expand: String = {
      if (clause.contains("__all__")) {
        val all = params.map { _ => "? " }.mkString(",")
        clause.replace("__all__", all)
      } else clause
    }
    def apply(prms: Any*): Where = Where(clause = clause, params = prms: _*)
  }

  class WhereBuilder(sql: String) {
    def using(params: Any*): Where = Where(sql, params: _*)
  }
  def where(sql: String): WhereBuilder = new WhereBuilder(sql)
  def where(sql: String, params: Any*): Where = Where(sql, params: _*)

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
      val massaged = if(typeOf[T] == typeOf[Array[Byte]]) blobToBytes(rawVal.asInstanceOf[JDBCBlobClient])
      else if (typeOf[T] == typeOf[InputStream]) blobToStream(rawVal.asInstanceOf[JDBCBlobClient])
      else rawVal

      massaged.asInstanceOf[T]
    }

    private def blobToStream(jDBCBlobClient: JDBCBlobClient): InputStream = jDBCBlobClient.getBinaryStream
    private def blobToBytes(jDBCBlobClient: JDBCBlobClient): Array[Byte]= jDBCBlobClient.getBytes(1, jDBCBlobClient.length.toInt)

    override def toString: String = {
      asMap.foldLeft("") { case (a, (k, v)) => a + s" Key:${k}, Value: ${v}" }
    }
  }
}