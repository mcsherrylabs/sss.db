package sss.db.ops

import sss.db.{Db, FutureTx}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object DbOps {


  implicit class DbRunOps[T](val futTx: FutureTx[T]) extends AnyVal {

    def dbRunSync(implicit db: Db): Try[T] = {
      futTx.runSync(db.syncRunContext)
    }

    def dbRunSyncGet(implicit db: Db): T = dbRunSync.get

    def dbRun(implicit db: Db): Future[T] = {
      futTx.run(db.asyncRunContext)
    }
  }

  implicit class OptFutureTxOps[T](val optFutTx: Option[FutureTx[T]]) extends AnyVal {
    def toFutureTxOpt(implicit db: Db): FutureTx[Option[T]] = context => {
      import db.asyncRunContext.ec
      optFutTx match {
        case Some(f) => f(context).map(Option.apply)
        case None => Future.successful(None)
      }
    }

    def unwrapOpt: FutureTx[T] = context => {
      optFutTx match {
        case Some(f) => f(context)
        case None => Future.failed(new IllegalArgumentException("unwrapOpt was empty"))
      }
    }
  }

  implicit class TryFutureTxOps[T](val tryFutTx: Try[FutureTx[T]]) extends AnyVal {
    def toFutureTxTry(implicit db: Db): FutureTx[Try[T]] = context => {
      import db.asyncRunContext.ec
      tryFutTx match {
        case Success(f) => f(context).map(Try(_))
        case Failure(e) => Future.successful(Failure(e))
      }
    }

    def unwrapTry: FutureTx[T] = context => {
      tryFutTx match {
        case Success(f) => f(context)
        case Failure(e) => Future.failed(e)
      }
    }

  }

  def recoverImpl[T, U >: T](fTx: FutureTx[T],
                             pf: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): FutureTx[U] = context => {
    fTx(context).recover(pf)
  }


  def recoverWithImpl[T, U >: T](fTx: FutureTx[T],
                                 pf: PartialFunction[Throwable, FutureTx[U]])(implicit ec: ExecutionContext): FutureTx[U] = context => {
    fTx(context).recoverWith {
      case e if pf.isDefinedAt(e) => pf.apply(e)(context)
    }
  }

  implicit class FutureTxOps[T](val futFutTx: FutureTx[T]) extends AnyVal {

    def recoverWith[U >: T](pf: PartialFunction[Throwable, FutureTx[U]])(implicit ec: ExecutionContext): FutureTx[U] =
      recoverWithImpl(futFutTx, pf)

    def recoverWithDb[U >: T](pf: PartialFunction[Throwable, FutureTx[U]])(implicit db: Db): FutureTx[U] = {
      import db.asyncRunContext.ec
      recoverWithImpl(futFutTx, pf)
    }


    def recover[U >: T](pf: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): FutureTx[U] =
      recoverImpl(futFutTx, pf)

    def recoverDb[U >: T](pf: PartialFunction[Throwable, U])(implicit db: Db): FutureTx[U] = {
      import db.asyncRunContext.ec
      recoverImpl(futFutTx, pf)
    }
  }

  implicit class FutureTxFutureTxOps[T](val futFutTx: FutureTx[FutureTx[T]]) extends AnyVal {
    def flattenFutureTx(ff: FutureTx[FutureTx[T]])(implicit ec: ExecutionContext): FutureTx[T] = context => {
      ff(context).flatMap(f => f(context))
    }
  }

  implicit class EitherFutureTxOps[L, T](val eitherFutTx: Either[L, FutureTx[T]]) extends AnyVal {
    def toFutureTxEither(implicit db: Db): FutureTx[Either[L, T]] = context => {
      import db.asyncRunContext.ec
      eitherFutTx match {
        case Right(f) => f(context).map(Right.apply)
        case Left(e) => Future.successful(Left(e))
      }
    }

    def unwrapEither: FutureTx[T] = context => {
      eitherFutTx match {
        case Right(f) => f(context)
        case Left(e: Exception) => Future.failed(e)
        case Left(e) => Future.failed(new RuntimeException(e.toString))
      }
    }

  }

}
