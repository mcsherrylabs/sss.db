package sss.db

import java.sql.Connection

import javax.sql.DataSource
import sss.ancillary.Logging

import scala.util.{Failure, Success, Try}

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

  def tx[T](isoLevel: Int, f: => Try[T]): Try[T] = inTransactionImpl[T](f)(startTx(isoLevel))

  def tx[T](isoLevel: Int, f: => T): T = inTransaction[T](isoLevel, f)

  def tx[T](f: => Try[T]): Try[T] = inTransactionImpl[T](f)(startTx())

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

  def validateTx[T](f: => T): Try[T] = validateTx[T](None)(f)

  def inTransaction[T](isoLevel:Int, f: => T): T =
    inTransactionImpl(Try(f))(startTx(isoLevel)).get

  def inTransaction[T](f: => T): T =
    inTransactionImpl(Try(f))(startTx()).get

  private def inTransactionImpl[T](f: => Try[T])(sTx: => Boolean): Try[T] = {
    val isNew = sTx
    val r = f
    r match {
      case Failure(e) => conn.rollback
      case Success(_) => if (isNew) conn.commit()
    }
    closeTx
    r
  }

}