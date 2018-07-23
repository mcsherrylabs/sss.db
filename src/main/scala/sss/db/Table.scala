package sss.db

import java.sql.{Connection, Statement}
import javax.sql.DataSource

import sss.ancillary.Logging

import scala.util.control.NonFatal

trait Tx extends Logging {

  private[db] val ds: DataSource
  private[db] def conn: Connection = Tx.get.conn

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
        log.debug("ROLLING BACK!", e)
        conn.rollback
        throw e
    } finally {
      closeTx
    }

  }
}

private[db] case class ConnectionTracker(conn: Connection, count: Int)

private[db] object Tx extends ThreadLocal[ConnectionTracker]

class Table private[db] ( name: String,
             ds: DataSource,
             freeBlobsEarly: Boolean,
             columns: String = "*")

  extends View(
    name,
    ds,
    freeBlobsEarly,
    columns) {

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
      apply(values(id).asInstanceOf[Number].longValue())
    } finally {
      ps.close
    }
  }

  /**
    * If an id is provided it will overwrite the generated identity
    * Either remove the id for genuine inserts or use 'persist' for
    * the best of both worlds.
    *
    * @param values
    * @return
    */
  def insert(values: Map[String, Any]): Row = inTransaction {

    val names = values.keys.mkString(",")
    val params = (0 until values.keys.size).map(x => "?").mkString(",")

    val sql = s"INSERT INTO ${name} (${names}) VALUES ( ${params})"
    val ps = prepareStatement(sql, values.values.toSeq, Some(Statement.RETURN_GENERATED_KEYS))
    try {
      ps.executeUpdate() // run the query
      val ks = ps.getGeneratedKeys
      ks.next
      get(ks.getLong(1)).getOrElse(DbError(s"Could not retrieve generated id of row just written to ${name}"))
    } finally {
      ps.close
    }

  }

  /**
    * Use persist to either update or insert.
    *
    * If id exists in the Map and is not 0 then it's considered an update
    * If id doesn't exist in the map or in the special case where it's 0 to
    * facilitate case classes with an id default value of 0 - it's considered an insert.
    *
    * @example
    *          case class MyRecord(name : String, id: Int = 0)
    *          val row = table.persist(MyRecord("Tony"))
    *          val tony = MyRecord(row)
    *          assert(tony.name == "Tony")
    *          assert(tony.id != 0)
    *          val updatedRow = table.persist(tony.copy(name = "Karl")
    *          val karl = MyRecord(updatedRow)
    *          assert(tony.id == karl.id)
    *
    * @param values
    * @return
    */
  def persist(values: Map[String, Any]): Row = inTransaction {

    values.partition(kv => id.equalsIgnoreCase(kv._1)) match {
      case (mapWithId, rest) if(mapWithId.isEmpty)       => insert(rest)
      case (mapWithId, rest) if(mapWithId.head._2 == 0l) => insert(rest)
      case _                                             => update(values)
    }
  }

  def delete(where: Where): Int = tx[Int] {

    val ps = prepareStatement(s"DELETE FROM ${name} WHERE ${where.clause}", where.params)
    try {
      ps.executeUpdate(); // run the query
    } finally {
      ps.close()
    }
  }

  /**
    * Update the table using a filter
    *
    * @param values the values to update
    * @param filter the 'where' clause (minus the where)
    * @return usually the number of rows updated.
    * @note This is a gateway for sql injection attacks, Use update(Map[]) if possible.
    * @example update("count = count + 1", "id = 1")
    */
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
