package sss.db.datasource

import com.typesafe.config.Config
import sss.ancillary.DynConfig


object DataSource {

  type CloseableDataSource = javax.sql.DataSource with AutoCloseable

  def apply(dsConfig: DataSourceConfig) :CloseableDataSource = HikariDataSource(dsConfig)
  def apply(dsConfig: Config) :CloseableDataSource = apply(DynConfig[DataSourceConfig](dsConfig))
  def apply(dbConfigName: String = "database.datasource") : CloseableDataSource =
    apply(DynConfig[DataSourceConfig](dbConfigName))

}

trait DataSourceConfig {
  val testQueryOpt: Option[String]
  val maxPoolSize: Int
  val cachePrepStmts : Boolean
  val prepStmtCacheSize: Int
  val prepStmtCacheSqlLimit: Int
  val useServerPrepStmts: Boolean
  val driver: String
  val connection: String
  val connectionProperties: String
  val user: String
  val pass: String
  val transactionIsolationLevel: String
  val connectionTimeout: Int
  val allowPoolSuspensionOpt: Option[Boolean]
  val poolName: String
  val connectionInitSqlOpt: Option[String]
  val catalogOpt: Option[String]
}

