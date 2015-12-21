package sss.db

import java.sql.{ Connection, Statement }
import javax.sql.DataSource

import scala.util.control.NonFatal

trait Tx {

  self: View =>

  def startTx: Boolean = {
    Option(Tx.get()) match {
      case None =>
        // auto commit should be off by default
        Tx.set(ConnectionTracker(ds.getConnection, 0))
        true
      case Some(existing) =>
        Tx.set(ConnectionTracker(existing.conn, existing.count + 1))
        false
    }

  }

  def closeTx = {
    val existing = Tx.get()
    if (existing == null) throw new IllegalStateException("Closing a non existing tx?")
    else {
      if (existing.count == 0) {
        existing.conn.close
        Tx.remove
      } else {
        Tx.set(ConnectionTracker(existing.conn, existing.count - 1))
      }

    }

  }

  def tx[T](f: => T): T = inTransaction[T](f)

  def inTransaction[T](f: => T): T = {
    val isNew = startTx
    try {
      val r = f
      if (isNew) conn.commit()
      r
    } catch {
      case NonFatal(e) =>
        log.warn("ROLLING BACK!", e)
        conn.rollback
        throw e
    } finally {
      closeTx
    }

  }
}

private[db] case class ConnectionTracker(conn: Connection, count: Int)

private[db] object Tx extends ThreadLocal[ConnectionTracker]

class Table(name: String, ds: DataSource) extends View(name, ds) {

  @throws[DbOptimisticLockingException]("if the row has been updated after you read it")
  def update(values: Map[String, Any]): Row = inTransaction {

    val minusVersion = values.filterNot { case (n: String, v) => version.equalsIgnoreCase(n) }

    val usingVersion = values.size == minusVersion.size + 1

    if (values.size - minusVersion.size > 1) {
      throw DbException(s"There are multiple 'version' fields in ${values}, cannot update ${name}")
    }

    val params = minusVersion.map { case (k: String, v) => s"$k=?" }.mkString(",")

    val (sql, p) = if (usingVersion) {
      (s"UPDATE ${name} SET ${params}, version = version + 1 WHERE id = ? AND version = ?", minusVersion.values.toSeq :+ values(id) :+ values(version))
    } else {
      (s"UPDATE ${name} SET ${params} WHERE id = ?", minusVersion.values.toSeq :+ values(id))
    }

    val ps = prepareStatement(sql, p)

    try {
      val numRows = ps.executeUpdate()
      if (usingVersion && numRows == 0) throw new DbOptimisticLockingException(s"No rows were updated, optimistic lock clash? ${name}:${values}")
      apply(values(id).asInstanceOf[Long])
    } finally {
      ps.close
    }
  }

  def insert(values: Map[String, Any]): Row = inTransaction {

    val minusId = values.filterNot { case (n: String, v) => id.equalsIgnoreCase(n) }

    val names = minusId.keys.mkString(",")
    val params = (0 until minusId.keys.size).map(x => "?").mkString(",")

    val sql = s"INSERT INTO ${name} (${names}) VALUES ( ${params})"
    val ps = prepareStatement(sql, minusId.values.toSeq, Some(Statement.RETURN_GENERATED_KEYS))
    try {
      ps.executeUpdate() // run the query
      val ks = ps.getGeneratedKeys
      ks.next
      get(ks.getLong(1)).getOrElse(DbError(s"Could not retrieve generated id of row just written to ${name}"))
    } finally {
      ps.close
    }

  }

  def persist(values: Map[String, Any]): Row = inTransaction {

    values.get(id) match {
      case None | Some(0l) => insert(values)
      case Some(existingId) => update(values)
    }
  }

  def delete(where: Where): Int = tx[Int] {

    val ps = prepareStatement(s"DELETE FROM ${name} WHERE ${where.expand}", where.params)
    try {
      ps.executeUpdate(); // run the query
    } finally {
      ps.close()
    }
  }

  @deprecated("This does not use a prepared statement and is unsafe (SQL injection attack)", "0.9.2")
  def update(values: String, filter: String): Int = tx {

    val st = conn.createStatement(); // statement objects can be reused with
    try {

      val sql = s"UPDATE ${name} SET ${values} WHERE ${filter}"
      st.executeUpdate(sql); // run the query

    } finally {
      st.close
    }
  }

  def insert(values: Any*): Int = tx {

    val params = (0 until values.size).map(x => "?").mkString(",")
    val sql = s"INSERT INTO ${name} VALUES ( ${params})"
    val ps = prepareStatement(sql, values.toSeq, Some(Statement.RETURN_GENERATED_KEYS))
    try {
      ps.executeUpdate(); // run the query
    } finally {
      ps.close
    }
  }

}
