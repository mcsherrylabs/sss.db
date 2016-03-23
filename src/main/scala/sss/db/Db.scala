package sss.db

import java.sql.{Connection, SQLException}
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import sss.ancillary.{DynConfig, Logging}

import scala.collection.JavaConversions._
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
  val cachePrepStmts : Boolean
  val prepStmtCacheSize: Int
  val prepStmtCacheSqlLimit: Int
  val useServerPrepStmts: Boolean
  val driver: String
  val connection: String
  val user: String
  val pass: String
  val useShutdownHook: Boolean
  val deleteSqlOpt: Option[java.lang.Iterable[String]]
  val createSqlOpt: Option[java.lang.Iterable[String]]

}

class Db(dbConfig: DbConfig) extends Logging with Dynamic with Tx {

  private val tables = new ConcurrentHashMap[String, Table]()

  override private[db] def conn: Connection = Tx.get.conn

  if(dbConfig.useShutdownHook) sys addShutdownHook shutdown

  // Load the HSQL Database Engine JDBC driver
  // hsqldb.jar should be in the class path or made part of the current jar
  //Class.forName(dbConfig.driver) // "org.hsqldb.jdbc.JDBCDriver");

  private[db] val ds = setUpDataSource(dbConfig)

  {

    dbConfig.deleteSqlOpt foreach { deleteSqlAry =>

      deleteSqlAry.foreach { deleteSql =>
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
      createSqlAry foreach { createSql =>
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

  def table(name: String): Table = {
    Option(tables.get(name)) match {
      case None => {
        tables.putIfAbsent(name, new Table(name, ds))
        tables.get(name)
      }
      case Some(t) => t
    }
  }

  def view(name: String): View = new View(name, ds)

  def dropView(viewName: String) = executeSql(s"DROP VIEW ${viewName}")

  def createView(createViewSql: String) = executeSql(createViewSql)

  def shutdown = {
    executeSql("SHUTDOWN")
    ds.close
  }

  def executeSqls(sqls: Seq[String]): Seq[Int] = sqls.map(executeSql(_))

  def executeSql(sql: String):Int = inTransaction {
    val st = conn.createStatement()
    try {
      st.executeUpdate(sql)
    } finally st.close
  }

  private def setUpDataSource(dbConfig: DbConfig) = {

    val hikariConfig = new HikariConfig()
    hikariConfig.setDriverClassName(dbConfig.driver)
    hikariConfig.setJdbcUrl(dbConfig.connection)
    hikariConfig.setUsername(dbConfig.user)
    hikariConfig.setPassword(dbConfig.pass)
    hikariConfig.setAutoCommit(false)

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