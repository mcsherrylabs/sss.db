package sss.db
import java.sql.Connection

import scala.concurrent.{ExecutionContext, Future}

case class TransactionContext(conn: Connection, implicit val ec: ExecutionContext)

trait FutureTx[T] extends (TransactionContext => Future[T]) {

  def flatMap[C](t: T => FutureTx[C]): FutureTx[C] = context => {
    import context.ec
    val g: Future[TransactionContext => Future[C]] = apply(context) map t
    g flatMap (_(context))
  }

  def map[C](t: T => C): FutureTx[C] = context => {
    import context.ec
    apply(context) map t
  }
}

object FutureTx {
  def unit[A](a: A): FutureTx[A] = conn => Future.successful(a)

  def sequence[T](seqT: Seq[FutureTx[T]]): FutureTx[Seq[T]] = {
    seqT.foldLeft[FutureTx[Seq[T]]](FutureTx.unit(Seq.empty[T])) {
      (acc,e) => acc.flatMap(sq => e.map (_  +: sq))
    }
  }
}