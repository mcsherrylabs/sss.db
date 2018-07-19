package sss.db

import java.sql.{Connection, SQLException}

import com.twitter.util.SynchronizedLruMap
import com.typesafe.config.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import sss.ancillary.{DynConfig, Logging}
import collection.JavaConverters._

import scala.collection.mutable.{Map => MMap}
import scala.language.dynamics

object Db {

  def apply(dbConfig: DbConfig) = {
    new Db(dbConfig)
  }

  def apply(dbConfig: Config): Db = {
    apply(DynConfig[DbConfig](dbConfig))
  }

  def apply(dbConfigName: String = "database"): Db = {
    apply(DynConfig[DbConfig](dbConfigName))
  }
}

trait DbConfig {
  val testQueryOpt: Option[String]
  val maxPoolSize: Int
  val freeBlobsEarly: Boolean
  val cachePrepStmts : Boolean
  val prepStmtCacheSize: Int
  val prepStmtCacheSqlLimit: Int
  val useServerPrepStmts: Boolean
  val driver: String
  val connection: String
  val connectionProperties: String
  val user: String
  val pass: String
  val useShutdownHook: Boolean
  val tableCacheSize: Int
  val deleteSqlOpt: Option[java.lang.Iterable[String]]
  val createSqlOpt: Option[java.lang.Iterable[String]]

}

class Db(dbConfig: DbConfig) extends Logging with Dynamic with Tx {

  private lazy val tables: MMap[String, Table] = new SynchronizedLruMap[String, Table](dbConfig.tableCacheSize)


  override private[db] def conn: Connection = Tx.get.conn

  if(dbConfig.useShutdownHook) sys addShutdownHook shutdown

  private[db] val ds = setUpDataSource(dbConfig)

  {

    dbConfig.deleteSqlOpt foreach { deleteSqlAry =>

      deleteSqlAry.asScala foreach { deleteSql =>
        if (deleteSql.length > 0) {
          try {
            val deleted = executeSql(deleteSql)
            log.info(s"${deleteSql} Deleted count ${deleted}")
          } catch {
            case e: SQLException => log.warn(s"${deleteSql} failed, maybe object doesn't exist?!", e)
          }
        }
      }

    }
    dbConfig.createSqlOpt foreach { createSqlAry =>
      createSqlAry.asScala foreach { createSql =>
        if (createSql.length > 0) {
          try {
            val created = executeSql(createSql)
            log.info(s"${createSql} Created count ${created}")
          } catch {
            case e: SQLException => log.warn(s"Failed to create ${createSql}")
          }
        }
      }
    }

  }

  def selectDynamic(tableName: String) = table(tableName)

  def table(name: String): Table =  tables.getOrElseUpdate(name, new Table(name, ds, dbConfig.freeBlobsEarly))

  def view(name: String): View = new View(name, ds, dbConfig.freeBlobsEarly)

  def dropView(viewName: String) = executeSql(s"DROP VIEW ${viewName}")

  def createView(createViewSql: String) = executeSql(createViewSql)

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
  def executeSqls(sqls: Seq[String]): Seq[Int] = inTransaction(sqls.map(executeSql(_)))

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

  private def setUpDataSource(dbConfig: DbConfig) = {

    val hikariConfig = new HikariConfig()
    hikariConfig.setDriverClassName(dbConfig.driver)
    val connPlusProps = {
      if(!dbConfig.connectionProperties.startsWith(";") && dbConfig.connectionProperties.length > 0) {
        dbConfig.connection + ";" + dbConfig.connectionProperties

      } else dbConfig.connection + dbConfig.connectionProperties
    }
    hikariConfig.setJdbcUrl(connPlusProps)

    hikariConfig.setUsername(dbConfig.user)
    hikariConfig.setPassword(dbConfig.pass)
    hikariConfig.setAutoCommit(false)
    hikariConfig.setIsolateInternalQueries(true)

    hikariConfig.setMaximumPoolSize(dbConfig.maxPoolSize)
    dbConfig.testQueryOpt map (hikariConfig.setConnectionTestQuery(_))
    hikariConfig.setPoolName("hikariCP")


    hikariConfig.addDataSourceProperty("dataSource.cachePrepStmts", dbConfig.cachePrepStmts)
    hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSize", dbConfig.prepStmtCacheSize)
    hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", dbConfig.prepStmtCacheSqlLimit)
    hikariConfig.addDataSourceProperty("dataSource.useServerPrepStmts", dbConfig.useServerPrepStmts)

    new HikariDataSource(hikariConfig)

  }


}