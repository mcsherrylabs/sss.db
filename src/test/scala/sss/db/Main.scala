package sss.db

import java.sql.Connection

import scala.concurrent.Future

trait Q {
  def conn: Connection = ???

  def tx[T](f: => Connection => T): T = {
    f(conn)
  }

  def insert(v: Map[String, Any])(implicit conn: Connection): Future[Row]
  def find(w: Where): Option[Row]
}

class Main {

  def main(args: Array[String]): Unit = {

    val q: Q = ???

    val r: Option[Row] = Transaction {
      q.find(where())
    }()



  }
}
