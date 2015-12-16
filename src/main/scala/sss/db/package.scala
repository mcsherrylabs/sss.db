package sss

/**
 * @author alan
 */
package object db {

  private def flattenToMap(cc: AnyRef) =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc))
    }

  object DbException { def apply(msg: String) = throw new DbException(msg) }
  object DbError { def apply(msg: String) = throw new DbError(msg) }

  class DbException(msg: String) extends RuntimeException(msg)
  class DbError(msg: String) extends Error(msg)

  type Rows = IndexedSeq[Row]

  //import scala.language.dynamics
  //import scala.reflect.runtime.universe._

  implicit class Row(private val map: Map[String, Any]) {

    override def equals(o: Any) = o match {
      case that: Row => that.map.equals(map)
      case x => false
    }

    override def hashCode = map.hashCode

    def apply[T](col: String): T = {
      map(col.toUpperCase).asInstanceOf[T]
    }

    override def toString: String = {
      map.foldLeft("") { case (a, (k, v)) => a + s" Key:${k}, Value: ${v}" }
    }
  }
}