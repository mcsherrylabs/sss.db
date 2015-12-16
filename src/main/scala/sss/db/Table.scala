package sss.db

import java.sql.Connection
import java.util.Date
import util.control.Exception.allCatch
import javax.sql.DataSource
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import sss.ancillary.Logging
import scala.util.control.NonFatal
import java.sql.Statement

private[db] trait Tx {
  def inTransaction[T](f: => T): T
  def startTx: Boolean
  def closeTx
}

private[db] case class ConnectionTracker(conn: Connection, count: Int)

private[db] object Tx extends ThreadLocal[ConnectionTracker]

/**
 * SQL INJECTION ATTACKS .... CHANGE THE STATEMENTS
 */
class Table(val name: String, val ds: DataSource) extends Tx with Logging {

  implicit def conn: Connection = Tx.get.conn

  def startTx: Boolean = {
    val existing = Tx.get()
    if (existing == null) {
      // auto commit should be off by default
      Tx.set(ConnectionTracker(ds.getConnection(), 0))
      true
    } else {
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

  private def getRowTx(sql: String): Option[Row] = {
    val rows = filterTx(sql)

    rows.size match {
      case 0 => None
      case 1 => Some(rows(0))
      case size => throw new Error(s"Too many ${size}")
    }
  }

  private def getRowTx(id: Long): Option[Row] = getRowTx(s"id = ${id}")

  private def mapTx[B](f: Row => B): IndexedSeq[B] = {

    val st = conn.createStatement(); // statement objects can be reused with
    try {
      val rs = st.executeQuery(s"SELECT * FROM ${name}"); // run the query
      Rows(rs).map(f)
    } finally {
      st.close
    }
  }

  private def deleteTx(sql: String): Int = {

    val st = conn.createStatement(); // statement objects can be reused with
    try {
      st.executeUpdate(s"DELETE FROM ${name} WHERE ${sql}"); // run the query	       
    } finally {
      st.close()
    }
  }

  private def filterTx(sql: String): Rows = {

    val st = conn.createStatement(); // statement objects can be reused with
    try {
      val rs = st.executeQuery(s"SELECT * FROM ${name} WHERE ${sql}"); // run the query
      Rows(rs)
    } finally {
      st.close
    }
  }

  private def updateTx(values: String, filter: String): Int = {

    val st = conn.createStatement(); // statement objects can be reused with
    try {

      val sql = s"UPDATE ${name} SET ${values} WHERE ${filter}"
      st.executeUpdate(sql); // run the query

    } finally {
      st.close
    }
  }

  private def mapToSql(value: Any): Any = {
    value match {
      case v: String => s"'${v}'"
      case v: Date => v.getTime
      case null => "null"
      case Some(x) => mapToSql(x)
      case None => "null"
      case v => v
    }
  }

  private def insertTx(values: Any*): Int = {
    val st = conn.createStatement(); // statement objects can be reused with
    try {
      val asStrs = values map (mapToSql(_))

      val str = asStrs.mkString(", ")
      val sql = s"INSERT INTO ${name} VALUES ( ${str})"
      st.executeUpdate(sql); // run the query

    } finally {
      st.close
    }
  }

  def getRow(sql: String): Option[Row] = inTransaction[Option[Row]](getRowTx(sql))

  def getRow(id: Long): Option[Row] = inTransaction[Option[Row]](getRowTx(id))

  def map[B](f: Row => B): IndexedSeq[B] = inTransaction[IndexedSeq[B]](mapTx(f))

  def delete(sql: String): Int = inTransaction[Int](deleteTx(sql))

  /*
   * Need distributedTo and distributedFrom columns
   * on client start up find last distributed from 
   * and GET /distribute/clinetNodeId/taskType/UUID
   * then save task to table including distributedFrom
   * 
   * Server looks for task with task id of client UUID
   * and distributed node id, this is deleted.
   * If deletedCount == 0 it means we're out of sync.
   * In that case seek a distributedTo and return it. 
   * In the normal case, checkout a task in a transaction/synchronized 
   * and return that. 
   * 
   *         
   */
  /*def toStream(sql: String, bufferSize: Int = 10): Stream[Option[Row]] = {

    def fetchBuffer(): Option[Row] = {
      //filter(s"${sql} LIMIT ${bufferSize}")
      None
    }

    Stream.cons(fetchBuffer(), fetchBuffer _)
  }*/

  def filter(sql: String): Rows = {
    inTransaction(filterTx(sql))
  }

  def update(values: String, filter: String): Int = {
    inTransaction(updateTx(values, filter))
  }

  def insert(values: Any*): Int = {
    inTransaction(insertTx(values: _*))
  }

  def update(values: Map[String, Any]) = inTransaction {
    val st = conn.createStatement(); // statement objects can be reused with
    try {
      val params = (values.foldLeft("") { (acc, e) => (s"${acc}, ${e._1}=${mapToSql(e._2)}") }).substring(1)

      val sql = s"UPDATE ${name} SET ${params} WHERE id = ${mapToSql(values("id"))}"
      val numRows = st.executeUpdate(sql); // run the query
      numRows
    } finally {
      st.close
    }
  }

  def insert(values: Map[String, Any]): Row = inTransaction {

    val (names, params) = values.foldLeft(("", "")) { (acc, e) => (s"${acc._1}, ${e._1}", s"${acc._2}, ${mapToSql(e._2)}") }
    val sql = s"INSERT INTO ${name} (${names.substring(1)}) VALUES ( ${params.substring(1)})"
    val st = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    try {
      st.executeUpdate() // run the query
      val ks = st.getGeneratedKeys
      ks.next
      getRow(ks.getInt(1)).getOrElse(DbError(s"Could not retrieve generated id of row just written to ${name}"))
    } finally {
      st.close
    }

  }

  def persist(values: Map[String, Any]): Row = inTransaction {
    values.get("id") match {
      case None | Some(0) => insert(values)
      case Some(existingId) => update(values); getRow(existingId.asInstanceOf[Int]).getOrElse(DbError(s"Could not retrieve row just written ${existingId}"))
    }
  }

}