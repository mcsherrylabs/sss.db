package sss.db.experiment

import sss.db.Where
import sss.db.experiment.SqlBuilder.{JoinBuilder, OnBuilder, Query}


case class Table(name: String) extends JoinBuilder {

  override def join(t:Table): OnBuilder = {
    SqlBuilder.Join(this, t)
  }
}

object SqlBuilder {

  trait JoinBuilder {
    def join(table: Table): OnBuilder
  }

  trait OnBuilder {
    def on(where: Where): Query
  }

  trait Query {
    def sql: String
    def sql(where: Where): String
    val where: Where
  }

  def Join(t1: Table, t2: Table): OnBuilder = {
    (whre: Where) => new Query{
        override def sql: String = ???
        override def sql(where: Where): String = ???
        override val where: Where = whre
    }
  }
}


object Experiment {


  def main(args: Array[String]): Unit = {


    val a = Table("a")
    val b = Table("b")

    val q: Query = a join b on Where("id = b.node_id")


    "SELECT * from a JOIN b on a.id = b.node_id"

  }
}
