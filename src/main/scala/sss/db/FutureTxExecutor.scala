package sss.db

import sss.ancillary.FutureOps.AwaitReady
import sss.db.TxIsolationLevel.TxIsolationLevel

import java.sql.Connection
import javax.sql.DataSource
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait FutureTxExecutor {

  def execute[T](fTx: FutureTx[T],
                 conn: Connection,
                 isRollback: Boolean,
                 ec: ExecutionContext,
                 isolationLevel: Option[TxIsolationLevel]): Future[T] = {

    implicit val ecImplicit = ec

    Try {
      isolationLevel.foreach(l => conn.setTransactionIsolation(l.id))
      fTx(TransactionContext(conn, ec))
    } match {
      case Failure(e) =>

        try conn.rollback()
        finally conn.close()

        Future.failed(e)

      case Success(result) =>
        result map { r =>
          try {
            if (isRollback) conn.rollback()
            else conn.commit()
            r
          } finally conn.close()

        } recoverWith { case e =>

          try conn.rollback()
          finally conn.close()

          Future.failed[T](e)
        }

    }
  }

  def execute[T](fTx: FutureTx[T], runContext: RunContext, isRollback: Boolean): Future[T] = {
    execute(fTx, runContext.ds.getConnection, isRollback, runContext.ec, runContext.isolationLevel)
  }


  def executeSync[T](fTx: FutureTx[T],
                     ds: DataSource,
                     isRollback: Boolean, isolationLevel: Option[TxIsolationLevel]): Try[T] = {
    val ec = ExecutionContextHelper.synchronousExecutionContext
    execute(fTx, ds.getConnection, isRollback, ec, isolationLevel).toTry(1.second)
  }


}

object FutureTxExecutor extends FutureTxExecutor