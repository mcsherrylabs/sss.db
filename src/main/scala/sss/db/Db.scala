package sss.db


import com.twitter.util.SynchronizedLruMap
import com.typesafe.config.Config
import javax.sql.{DataSource => SqlDataSource}
import sss.ancillary.{DynConfig, Logging}
import sss.db.datasource.DataSource
import sss.db.datasource.DataSource._

import scala.concurrent.Future


object Db {

  def apply(dbConfig: DbConfig)(ds:CloseableDataSource): Db = {
    new Db(dbConfig)(ds)
  }

  def apply(dbConfig: Config)(ds:CloseableDataSource): Db = {
    apply(DynConfig[DbConfig](dbConfig))(ds)
  }

  def apply(dbConfigName: String = "database", ds:CloseableDataSource = DataSource()): Db = {
    apply(DynConfig[DbConfig](dbConfigName))(ds)
  }
}

trait DbConfig {
  val freeBlobsEarly: Boolean
  val useShutdownHook: Boolean
  val viewCachesSize: Int
  val deleteSqlOpt: Option[java.lang.Iterable[String]]
  val createSqlOpt: Option[java.lang.Iterable[String]]
}

class Db(dbConfig: DbConfig)(closeableDataSource:CloseableDataSource) extends Logging {

  private lazy val viewCache  = new SynchronizedLruMap[String, View](dbConfig.viewCachesSize)
  private lazy val tableCache = new SynchronizedLruMap[String, Table](dbConfig.viewCachesSize)

  implicit val ds: SqlDataSource = closeableDataSource

  if(dbConfig.useShutdownHook) sys addShutdownHook shutdown

  DbInitialSqlExecutor(dbConfig: DbConfig, executeSql _)(ds)

  def table(name: String): Table =  tableCache.getOrElseUpdate(name, new Table(name, ds, dbConfig.freeBlobsEarly))

  /*
  Views - they're great!

  https://stackoverflow.com/questions/3854606/what-are-the-disadvantage-of-sql-views
  http://www.craigsmullins.com/viewnw.htm

  Note - views can be based on other views.
  Note - views can be updated with limitations
  (http://www.informit.com/articles/article.aspx?p=130855&seqNum=4)

  TL;DR for blockchain type applications views are a good solution.
   */
  def view(name: String): View = viewCache.getOrElseUpdate(name, new View(name, ds, dbConfig.freeBlobsEarly))

  def dropView(viewName: String): Transaction[Int] = executeSql(s"DROP VIEW ${viewName}")

  def createView(createViewSql: String): Transaction[Int] = executeSql(createViewSql)

  def select(sql: String): Query = new Query(sql, ds, dbConfig.freeBlobsEarly)


  def shutdown = {
    executeSql("SHUTDOWN") map (_ => closeableDataSource.close())
  }

  /**
    * Execute a sequence of sql statements each in a single transaction.
    *
    * @param sqls
    * @return
    * @note This is a gateway for sql injection attacks, use with extreme caution.
    */
  def executeSqls(sqls: Seq[String]): Transaction[Seq[Int]] = {
    sqls.foldLeft(Transaction.unit(Seq.empty[Int])) {
      (acc,e) => acc.flatMap(seqInt => executeSql(e).map(i => seqInt :+ i))
    }
  }

  /**
    * Execute any sql you give it on a db connection in a transaction
    * .
    * @param sql - sql to execute
    * @return
    * @note This is a gateway for sql injection attacks, use with extreme caution.
    */
  def executeSql(sql: String):Transaction[Int] = { context =>
    val st = context.conn.createStatement()
    try {
      Future(st.executeUpdate(sql))(context.ec)
    } finally st.close
  }


}