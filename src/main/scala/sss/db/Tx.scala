package sss.db

import java.sql.Connection

import javax.sql.DataSource
import sss.ancillary.Logging

import scala.util.Try
import scala.util.control.NonFatal

private[db] case class ConnectionTracker(conn: Connection, count: Int)

private[db] object Tx extends ThreadLocal[ConnectionTracker]

trait Tx extends Logging {

  private[db] val ds: DataSource
  private[db] def conn: Connection = Tx.get.conn


  def startTx(): Boolean = {
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

  def startTx(isolationLevel: Int): Boolean = {
    Option(Tx.get()) match {
      case None =>
        // auto commit should be off by default
        val c = ds.getConnection
        c.setTransactionIsolation(isolationLevel)
        Tx.set(ConnectionTracker(c, 0))
        true
      case Some(existing) =>

        require(existing.conn.getTransactionIsolation() == isolationLevel,
          s"Required isolation level ($isolationLevel) must be same as " +
            s"existing level (${existing.conn.getTransactionIsolation()})")

        Tx.set(ConnectionTracker(existing.conn, existing.count + 1))
        false
    }

  }

  def closeTx = {
    Option(Tx.get) match {
      case None => throw new IllegalStateException("Closing a non existing tx?")
      case Some(existing) if existing.count == 0 =>
        existing.conn.close
        Tx.remove
      case Some(existing) =>
        Tx.set(ConnectionTracker(existing.conn, existing.count - 1))
    }
  }

  def tx[T](f: => T): T = inTransaction[T](f)

  def validateTx[T](isolationLevel: Option[Int] = None)(f: => T): Try[T] = Try {

    val txStarted = isolationLevel map (startTx(_)) getOrElse (startTx())

    require(txStarted, "Must validate in standalone tx")

    try {
      f
    } finally {
      conn.rollback()
      closeTx
    }
  }

  def validateTx[T](f: => T): Try[T] = Try {

    require(startTx(), "Must validate in standalone tx")

    try {
      f
    } finally {
      conn.rollback()
      closeTx
    }
  }

  def inTransaction[T](f: => T): T = {
    val isNew = startTx()
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