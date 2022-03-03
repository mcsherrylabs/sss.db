package sss.db


import com.typesafe.config.Config
import sss.ancillary.{DynConfig, Logging, LoggingFutureSupport}
import sss.db.datasource.DataSource
import sss.db.datasource.DataSource._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt



object Db {

  def apply(dbConfig: DbConfig)(ds:CloseableDataSource,
                                executionContext: ExecutionContext): Db = {
    new Db(dbConfig)(ds, executionContext)
  }

  def apply(dbConfig: Config)(ds:CloseableDataSource,
                              executionContext: ExecutionContext): Db = {
    apply(DynConfig[DbConfig](dbConfig))(ds, executionContext)
  }

  def apply(dbConfigName: String = "database",
            ds:CloseableDataSource = DataSource(),
            executionContext: ExecutionContext = ExecutionContextHelper.ioExecutionContext): Db = {
    apply(DynConfig[DbConfig](dbConfigName))(ds, executionContext)
  }
}

trait DbConfig {
  val freeBlobsEarly: Boolean
  val useShutdownHook: Boolean
  val viewCachesSize: Int
  val deleteSqlOpt: Option[java.lang.Iterable[String]]
  val createSqlOpt: Option[java.lang.Iterable[String]]
}

class Db(dbConfig: DbConfig)(closeableDataSource:CloseableDataSource, ec: ExecutionContext)
  extends Logging
    with LoggingFutureSupport{


  if(dbConfig.useShutdownHook) sys addShutdownHook shutdown

  val defaultSyncContextTransactionTimeout = 3.seconds

  implicit val syncRunContext: SyncRunContext = new SyncRunContext(closeableDataSource, defaultSyncContextTransactionTimeout)
  implicit val asyncRunContext: AsyncRunContext = new AsyncRunContext(closeableDataSource, ec)

  DbInitialSqlExecutor(dbConfig: DbConfig, executeSql)(syncRunContext)

  def table(name: String): Table =  new Table(name, syncRunContext, dbConfig.freeBlobsEarly)

  /*
  Views - they're great!

  https://stackoverflow.com/questions/3854606/what-are-the-disadvantage-of-sql-views
  http://www.craigsmullins.com/viewnw.htm

  Note - views can be based on other views.
  Note - views can be updated with limitations
  (http://www.informit.com/articles/article.aspx?p=130855&seqNum=4)

  TL;DR for blockchain type applications views are a good solution.
   */
  def view(name: String): View = viewWhere(name, where())

  def viewWhere(name: String, where: Where): View = new View(name, where, syncRunContext, dbConfig.freeBlobsEarly)

  def dropView(viewName: String): FutureTx[Int] = executeSql(s"DROP VIEW ${viewName}")

  def createView(createViewSql: String): FutureTx[Int] = executeSql(createViewSql)

  def select(sql: String): Query = selectWhere(sql, where())

  def selectWhere(selectSql: String, where: Where) = new Query(selectSql, where, syncRunContext, dbConfig.freeBlobsEarly)

  def shutdown: FutureTx[Int] = {
    executeSql("SHUTDOWN") //.map{case x  => Try(closeableDataSource.close()); x}
  }

  /**
    * Execute a sequence of sql statements each in a single transaction.
    *
    * @param sqls
    * @return
    * @note This is a gateway for sql injection attacks, use with extreme caution.
    */
  def executeSqls(sqls: Seq[String]): FutureTx[Seq[Int]] = {
    sqls.foldLeft(FutureTx.unit(Seq.empty[Int])) {
      (acc,e) => acc.flatMap(seqInt => executeSql(e).map(i => seqInt :+ i))
    }
  }

  /**
    * Execute any sql you give it on a db connection in a transaction
    * .
    *
    * @param sql - sql to execute
    * @return
    * @note This is a gateway for sql injection attacks, use with extreme caution.
    */
  def executeSql(sql: String): FutureTx[Int] = { context =>
    LoggingFuture {
      val st = context.conn.createStatement()
      try {
        st.executeUpdate(sql)
      } finally st.close()
    }(context.ec)
  }


}