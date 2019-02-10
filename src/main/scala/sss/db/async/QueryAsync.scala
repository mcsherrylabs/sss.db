package sss.db.async

import java.sql.Connection

import sss.db._

import scala.collection.generic.FilterMonadic
import scala.concurrent.{ExecutionContext, Future}


trait Transaction[T] extends (Connection => T) {

  thisTx =>

  def flatMap[C](t:T => Transaction[C]): Transaction[C] = conn =>
    t(apply(conn))(conn)

  def map[C](t: T => C) : Transaction[C] = conn =>
    t(thisTx(conn))

  def run(c: Connection): T = apply(c)
}

case class Filter(where: Where) extends Transaction[Rows] {
  override def apply(c: Connection): Rows = {
    val ps = QueryA.prepareStatement(
      c,
      where.sql,
      where.params,
      None
    )

    try {
      val rs = ps.executeQuery
      Rows(rs, true)
    } finally ps.close
  }
}


object Ops {

  def insert(i: Int): Transaction[Int] = ???

  def find(w:Where): Transaction[Option[Row]] = {
    Filter(w) map { rows =>
      rows.size match {
        case 0 => None
        case 1 => Some(rows(0))
        case size => DbError(s"Should be 1 or 0, is -> ${size}")
      }
    }
  }
}

object Tx2 {

  def conn: Connection = ???

  def tx[T](f: => Transaction[T]): T = {
    f.run(conn)
  }
}

object Main {
  def main(args: Array[String]): Unit = {

    Tx2.tx {
      Ops.find(where())
    }

    val result: Transaction[Int] = Ops.find(where()) flatMap {
      case Some(row: Row) =>
        Ops.insert(row[Int]("id"))
    }

    result.run()
  }
}

class QueryAsync(q: Query)(implicit ec: ExecutionContext) extends Tx2 {

  def getRow(sql: Where): Transaction[Option[Row]] =
      Transaction(q.getRow(sql))



  def getRow(rowId: Long): Future[Option[Row]] = ???

  def map[B, W <% Where](f: Row => B, where: W): Future[QueryResults[B]] = ???

  def flatMap[B, W <% Where](f: Row => QueryResults[B], where: W): Future[QueryResults[B]] = ???

  def withFilter(f: Row => Boolean): Future[FilterMonadic[Row, IndexedSeq[Row]]] = ???

  def foreach[W <% Where](f: Row => Unit, where: W): Future[Unit] = ???

  def filter(where: Where): Future[Rows] = ???

  def filter(lookup: (String, Any)*): Future[Seq[Row]] = ???

  def find(lookup: (String, Any)*): Future[Option[Row]] = ???

  def find(sql: Where): Option[Row] = ???

  def apply(id: Long): Row = ???

  def get(id: Long): Option[Row] = ???

  def page(start: Long, pageSize: Int, orderClauses: Seq[OrderBy]): Rows = ???

  def toPaged(pageSize: Int, filter: Where, indexCol: String): PagedView = ???
}
