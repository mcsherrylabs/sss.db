package sss.db
import java.sql.Connection

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

case class TransactionContext(conn: Connection, implicit val ec: ExecutionContext)

trait FutureTx[+T] extends (TransactionContext => Future[T]) {

  def flatMap[C](t: T => FutureTx[C]): FutureTx[C] = context => {
    import context.ec
    val g: Future[TransactionContext => Future[C]] = apply(context) map t
    println("FutureTx flatMap")
    g flatMap (_(context))
  }

  def map[C](t: T => C): FutureTx[C] = context => {
    import context.ec
    println("FutureTx map")
    apply(context) map t
  }

  def withFilter(f: T => Boolean): FutureTx[T] = context => {
    import context.ec
    println("FutureTx withFilter")
    apply(context) withFilter f
  }


  def andAfter[U](pf: PartialFunction[Try[T], U]): FutureTx[T]  = context => {
    import context.ec
    println("FutureTx andAfter")
    apply(context).andThen(pf)
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