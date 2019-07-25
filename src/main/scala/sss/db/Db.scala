package sss.db

import com.twitter.util.SynchronizedLruMap
import com.typesafe.config.Config
import sss.ancillary.{DynConfig, Logging}
import sss.db.datasource.DataSource
import sss.db.datasource.DataSource._

import scala.language.dynamics

object Db {

  def apply(dbConfig: DbConfig)(ds:CloseableDataSource) = {
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

class Db(dbConfig: DbConfig)(private[db] val ds:CloseableDataSource) extends Logging with Dynamic with Tx {

  if(dbConfig.useShutdownHook) sys addShutdownHook shutdown

  DbInitialSqlExecutor(dbConfig: DbConfig, executeSql _)

  def selectDynamic(tableName: String) = table(tableName)

  def table(name: String): Table =  new Table(name, ds, dbConfig.freeBlobsEarly)

  /*
  Views - they're great!

  https://stackoverflow.com/questions/3854606/what-are-the-disadvantage-of-sql-views
  http://www.craigsmullins.com/viewnw.htm

  Note - views can be based on other views.
  Note - views can be updated with limitations
  (http://www.informit.com/articles/article.aspx?p=130855&seqNum=4)

  TL;DR for blockchain type applications views are a good solution.
   */
  def view(name: String): View = new View(name, ds, dbConfig.freeBlobsEarly)

  def dropView(viewName: String) = executeSql(s"DROP VIEW ${viewName}")

  def createView(createViewSql: String) = executeSql(createViewSql)

  def select(sql: String): Query = new Query(sql, ds, dbConfig.freeBlobsEarly)

  def shutdown = {
    executeSql("SHUTDOWN")
    ds.close
  }

  /**
    * Execute a sequence of sql statements each in a single transaction.
    *
    * @param sqls
    * @return
    * @note This is a gateway for sql injection attacks, use with extreme caution.
    */
  def executeSqls(sqls: Seq[String]): Seq[Int] = inTransaction(sqls.map(executeSql))

  /**
    * Execute any sql you give it on a db connection in a transaction
    * .
    * @param sql - sql to execute
    * @return
    * @note This is a gateway for sql injection attacks, use with extreme caution.
    */
  def executeSql(sql: String):Int = inTransaction {
    val st = conn.createStatement()
    try {
      st.executeUpdate(sql)
    } finally st.close
  }


}