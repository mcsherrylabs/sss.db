package sss.db.datasource

import com.zaxxer.hikari.{HikariConfig, HikariDataSource => HikariDS}
import sss.db.datasource.DataSource.CloseableDataSource

/**
  * The default and recommended data source.
  * (https://brettwooldridge.github.io/HikariCP/)
  *
  */
object HikariDataSource {

  def apply(dsConfig: DataSourceConfig): CloseableDataSource = {

    val hikariConfig = new HikariConfig()
    hikariConfig.setDriverClassName(dsConfig.driver)
    val connPlusProps = {
      if(!dsConfig.connectionProperties.startsWith(";") && dsConfig.connectionProperties.nonEmpty) {
        dsConfig.connection + ";" + dsConfig.connectionProperties

      } else dsConfig.connection + dsConfig.connectionProperties
    }
    hikariConfig.setJdbcUrl(connPlusProps)

    dsConfig.allowPoolSuspensionOpt foreach hikariConfig.setAllowPoolSuspension
    dsConfig.catalogOpt foreach hikariConfig.setCatalog
    dsConfig.connectionInitSqlOpt foreach hikariConfig.setConnectionInitSql

    hikariConfig.setConnectionTimeout(dsConfig.connectionTimeout)
    hikariConfig.setUsername(dsConfig.user)
    hikariConfig.setPassword(dsConfig.pass)
    hikariConfig.setAutoCommit(false)
    hikariConfig.setIsolateInternalQueries(true)
    hikariConfig.setTransactionIsolation(dsConfig.transactionIsolationLevel)

    hikariConfig.setMaximumPoolSize(dsConfig.maxPoolSize)
    dsConfig.testQueryOpt foreach hikariConfig.setConnectionTestQuery
    hikariConfig.setPoolName(dsConfig.poolName)


    hikariConfig.addDataSourceProperty("dataSource.cachePrepStmts", dsConfig.cachePrepStmts)
    hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSize", dsConfig.prepStmtCacheSize)
    hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", dsConfig.prepStmtCacheSqlLimit)
    hikariConfig.addDataSourceProperty("dataSource.useServerPrepStmts", dsConfig.useServerPrepStmts)

    new HikariDS(hikariConfig)

  }
}
