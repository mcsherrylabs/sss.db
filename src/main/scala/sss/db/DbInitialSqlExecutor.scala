package sss.db

import java.sql.SQLException
import sss.ancillary.Logging

import util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

object DbInitialSqlExecutor extends Logging {

  def apply(dbConfig: DbConfig, executeSql: String => Int): Unit = {

      dbConfig.deleteSqlOpt foreach { deleteSqlAry =>

        deleteSqlAry.asScala.filter(_.nonEmpty) foreach { deleteSql =>
          Try(executeSql(deleteSql)) match {
            case Failure(e: SQLException) => log.warn(s"${deleteSql} failed, maybe object doesn't exist?!", e)
            case Failure(e)               => throw e
            case Success(deleted)         => log.info(s"${deleteSql} Deleted count ${deleted}")
          }
        }
      }

      dbConfig.createSqlOpt foreach { createSqlAry =>
        createSqlAry.asScala.filter(_.nonEmpty) foreach { createSql =>
          Try(executeSql(createSql)) match {
            case Failure(e: SQLException) => log.warn(s"Failed to create ${createSql}")
            case Failure(e)               => throw e //fail fast
            case Success(created)         => log.info(s"${createSql} Created count ${created}")
          }
        }
      }
  }
}
