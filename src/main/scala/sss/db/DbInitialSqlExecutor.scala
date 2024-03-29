package sss.db

import java.sql.SQLException

import javax.sql.DataSource
import sss.ancillary.Logging

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object DbInitialSqlExecutor extends Logging {

  def apply(dbConfig: DbConfig, executeSql: String => FutureTx[Int])(implicit syncRunContext: SyncRunContext): Unit = {

      dbConfig.deleteSqlOpt foreach { deleteSqlAry =>

        deleteSqlAry.asScala.filter(_.nonEmpty) foreach { deleteSql =>
          Try(executeSql(deleteSql).runSync) match {
            case Failure(e: SQLException) => log.warn(s"${deleteSql} failed, maybe object doesn't exist?!", e)
            case Failure(e)               => throw e
            case Success(deleted)         => log.info(s"${deleteSql} Deleted count ${deleted}")
          }
        }
      }

      dbConfig.createSqlOpt foreach { createSqlAry =>
        createSqlAry.asScala.filter(_.nonEmpty) foreach { createSql =>
          Try(executeSql(createSql).runSync) match {
            case Failure(e: SQLException) => log.warn(s"Failed to create ${createSql}")
            case Failure(e)               => throw e //fail fast
            case Success(created)         => log.info(s"${createSql} Created count ${created}")
          }
        }
      }
  }
}
