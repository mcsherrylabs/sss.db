package sss.db

import java.sql.SQLException
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

import com.typesafe.config.Config
import org.apache.commons.dbcp2.{ DriverManagerConnectionFactory, PoolableConnectionFactory, PoolingDataSource }
import org.apache.commons.pool2.impl.GenericObjectPool
import sss.ancillary.{ DynConfig, Logging }

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
  val driver: String
  val connection: String
  val user: String
  val pass: String
  val deleteSqlOpt: Option[java.lang.Iterable[String]]
  val createSqlOpt: Option[java.lang.Iterable[String]]
}

class Db(dbConfig: DbConfig) extends Logging with Dynamic {

  private val tables = new ConcurrentHashMap[String, Table]()

  // Load the HSQL Database Engine JDBC driver
  // hsqldb.jar should be in the class path or made part of the current jar
  Class.forName(dbConfig.driver) // "org.hsqldb.jdbc.JDBCDriver");

  private val ds = setUpDataSource(dbConfig.connection,
    dbConfig.user,
    dbConfig.pass)

  {

    dbConfig.deleteSqlOpt foreach { deleteSqlAry =>

      deleteSqlAry.foreach { deleteSql =>
        if (deleteSql.length > 0) {
          val conn = ds.getConnection
          val st = conn.createStatement()
          try {
            val deleted = st.executeUpdate(deleteSql)
            log.info(s"${deleteSql} Deleted count ${deleted}")
          } catch {
            case e: SQLException => {
              log.warn(s"${deleteSql} failed, maybe object doesn't exist?!", e)
            }

          } finally {
            st.close
            conn.close
          }
        }
      }

    }
    dbConfig.createSqlOpt foreach { createSqlAry =>
      createSqlAry foreach { createSql =>
        if (createSql.length > 0) {
          val conn = ds.getConnection
          val st = conn.createStatement()
          try {
            val created = st.executeUpdate(createSql)
            log.info(s"${createSql} Created count ${created}")
          } catch {
            case e: SQLException => log.warn(s"Failed to create ${createSql}")

          } finally {
            st.close
            conn.close
          }
        }
      }
    }

  }

  def selectDynamic(tableName: String) = table(tableName)

  def table(name: String): Table = {
    tables.getOrElse(name, {
      val t = new Table(name, ds)
      tables.put(name, t)
      t
    })
  }

  def view(name: String): View = new View(name, ds)

  def dropView(viewName: String) = executeSql(s"DROP VIEW ${viewName}")

  def createView(createViewSql: String) = executeSql(createViewSql)

  def shutdown = executeSql("SHUTDOWN")

  private def executeSql(sql: String): Unit = {
    val conn = ds.getConnection
    val st = conn.createStatement()
    try {
      val created = st.executeUpdate(sql)
    } finally st.close
  }

  private def setUpDataSource(connectURI: String, user: String, pass: String): DataSource = {
    //
    // First, we'll create a ConnectionFactory that the
    // pool will use to create Connections.
    // We'll use the DriverManagerConnectionFactory,
    // using the connect string passed in the command line
    // arguments.
    //
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", pass)
    val connectionFactory = new DriverManagerConnectionFactory(connectURI, props)

    //
    // Next we'll create the PoolableConnectionFactory, which wraps
    // the "real" Connections created by the ConnectionFactory with
    // the classes that implement the pooling functionality.
    //
    val poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
    poolableConnectionFactory.setDefaultAutoCommit(false);

    //
    // Now we'll need a ObjectPool that serves as the
    // actual pool of connections.
    //
    // We'll use a GenericObjectPool instance, although
    // any ObjectPool implementation will suffice.
    //
    val connectionPool = new GenericObjectPool(poolableConnectionFactory)

    // Set the factory's pool property to the owning pool
    poolableConnectionFactory.setPool(connectionPool);

    //
    // Finally, we create the PoolingDriver itself,
    // passing in the object pool we created.
    //
    new PoolingDataSource(connectionPool)

  }
}