package sss.db
import java.sql.Connection

import scala.concurrent.{ExecutionContext, Future}

case class TransactionContext(conn: Connection, implicit val ec: ExecutionContext)

trait Transaction[T] extends (TransactionContext => Future[T]) {

  def flatMap[C](t: T => Transaction[C]): Transaction[C] = context => {
    import context.ec
    val g: Future[TransactionContext => Future[C]] = apply(context) map t
    g flatMap (_(context))
  }

  def map[C](t: T => C): Transaction[C] = context => {
    import context.ec
    apply(context) map t
  }
}

object Transaction {
  def unit[A](a: A): Transaction[A] = conn => Future.successful(a)
}