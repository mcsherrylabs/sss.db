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
      if(!dsConfig.connectionProperties.startsWith(";") && dsConfig.connectionProperties.length > 0) {
        dsConfig.connection + ";" + dsConfig.connectionProperties

      } else dsConfig.connection + dsConfig.connectionProperties
    }
    hikariConfig.setJdbcUrl(connPlusProps)

    hikariConfig.setUsername(dsConfig.user)
    hikariConfig.setPassword(dsConfig.pass)
    hikariConfig.setAutoCommit(false)
    hikariConfig.setIsolateInternalQueries(true)

    hikariConfig.setMaximumPoolSize(dsConfig.maxPoolSize)
    dsConfig.testQueryOpt map (hikariConfig.setConnectionTestQuery(_))
    hikariConfig.setPoolName("hikariCP")


    hikariConfig.addDataSourceProperty("dataSource.cachePrepStmts", dsConfig.cachePrepStmts)
    hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSize", dsConfig.prepStmtCacheSize)
    hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", dsConfig.prepStmtCacheSqlLimit)
    hikariConfig.addDataSourceProperty("dataSource.useServerPrepStmts", dsConfig.useServerPrepStmts)

    new HikariDS(hikariConfig)

  }
}
