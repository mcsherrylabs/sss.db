package sss.db


import com.twitter.util.SynchronizedLruMap
import com.typesafe.config.Config
import sss.ancillary.{DynConfig, Logging, LoggingFutureSupport}
import sss.db.TxIsolationLevel.TxIsolationLevel
import sss.db.datasource.DataSource
import sss.db.datasource.DataSource._

import scala.concurrent.ExecutionContext



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

  private lazy val viewCache  = new SynchronizedLruMap[String, View](dbConfig.viewCachesSize)
  private lazy val tableCache = new SynchronizedLruMap[String, Table](dbConfig.viewCachesSize)

  if(dbConfig.useShutdownHook) sys addShutdownHook shutdown

  DbInitialSqlExecutor(dbConfig: DbConfig, executeSql)(closeableDataSource)

  implicit val runContext: RunContext = new RunContext(closeableDataSource, ec)

  def runContext(level: TxIsolationLevel): RunContext = new RunContext(closeableDataSource, ec, Option(level))

  def table(name: String): Table =  tableCache.getOrElseUpdate(name, new Table(name, runContext, dbConfig.freeBlobsEarly))

  /*
  Views - they're great!

  https://stackoverflow.com/questions/3854606/what-are-the-disadvantage-of-sql-views
  http://www.craigsmullins.com/viewnw.htm

  Note - views can be based on other views.
  Note - views can be updated with limitations
  (http://www.informit.com/articles/article.aspx?p=130855&seqNum=4)

  TL;DR for blockchain type applications views are a good solution.
   */
  def view(name: String): View = viewCache.getOrElseUpdate(name, new View(name, runContext, dbConfig.freeBlobsEarly))

  def dropView(viewName: String): FutureTx[Int] = executeSql(s"DROP VIEW ${viewName}")

  def createView(createViewSql: String): FutureTx[Int] = executeSql(createViewSql)

  def select(sql: String): Query = new Query(sql, runContext, dbConfig.freeBlobsEarly)


  def shutdown: FutureTx[Int] = {
    executeSql("SHUTDOWN") //andAfter{case _  => closeableDataSource.close()}
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
    * @param sql - sql to execute
    * @return
    * @note This is a gateway for sql injection attacks, use with extreme caution.
    */
  def executeSql(sql: String):FutureTx[Int] = { context =>
    val st = context.conn.createStatement()
    try {
      LoggingFuture(st.executeUpdate(sql))(context.ec)
    } finally st.close()
  }


}