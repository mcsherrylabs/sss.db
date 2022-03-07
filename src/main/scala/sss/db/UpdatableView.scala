package sss.db

class UpdatableView private[db](name: String,
                                baseWhere: Where,
                                runContext: RunContext,
                                freeBlobsEarly: Boolean,
                                columns: String = "*")

  extends View(
    name,
    baseWhere,
    runContext,
    freeBlobsEarly,
    columns) {

  @throws[DbOptimisticLockingException]("if the row has been updated after you read it")
  def update(values: Map[String, Any], additionalWhere: Where, updateVersionCol: Boolean = false): FutureTx[Int] = {

    val where = baseWhere and additionalWhere
    val (keys, vals) = values.splitKeyValues

    val params = keys.map(k => s"$k = ?").mkString(",")

    val versionSql = if (updateVersionCol) ", version = version + 1" else ""
    val sql = s"UPDATE $name SET $params $versionSql ${where.sql}"

    prepareStatement(sql, vals ++ where.params).map { ps =>

      try {
        val numRows = ps.executeUpdate()
        if (updateVersionCol && numRows == 0) throw new DbOptimisticLockingException(s"No rows were updated, optimistic lock clash? ${name}:${values}:$where")
        numRows
      } finally {
        ps.close()
      }
    }
  }

  @throws[DbOptimisticLockingException]("if the row has been updated after you read it")
  def updateRow(values: Map[String, Any]): FutureTx[Row] = {

    val minusId = values - id

    val minusVersion = minusId.filterNot {
      case (n, _) => version.equalsIgnoreCase(n)
    }

    val usingVersion = minusId.size == minusVersion.size + 1

    if (minusId.size - minusVersion.size > 1) {
      throw DbException(s"There are multiple 'version' fields in ${values}, cannot update ${name}")
    }

    if (usingVersion) {
      for {
        _ <- update(minusVersion, where(id -> values(id)) and where("version" -> values(version)), true)
        r <- apply(values(id).asInstanceOf[Number].longValue())
      } yield r
    } else {
      for {
        _ <- update(minusVersion, where(id -> values(id)))
        r <- apply(values(id).asInstanceOf[Number].longValue())
      } yield r
    }

  }

  def delete(additionalWhere: Where): FutureTx[Int] = {
    val where = baseWhere and additionalWhere
    prepareStatement(s"DELETE FROM $name ${where.sql}", where.params) map { ps =>
      try {
        ps.executeUpdate(); // run the query
      } finally {
        ps.close()
      }
    }
  }

  /**
    * Update the table using a filter
    *
    * @param values the values to update
    * @param filter the 'where' clause (minus the where)
    * @return usually the number of rows updated.
    * @note This is a gateway for sql injection attacks, Use update(Map[]) if possible.
    * @example update("count = count + 1", "id = 1")
    */
  def update(values: String, filter: String): FutureTx[Int] = {
    val w = baseWhere and where(filter)
    val sql = s"UPDATE $name SET $values ${w.sql}"
    prepareStatement(sql, w.params) map { ps =>
      try ps.executeUpdate(sql) // run the query
      finally ps.close()
    }
  }

}
