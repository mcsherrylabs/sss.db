package sss.db

import java.sql.Statement

import javax.sql.DataSource

import scala.concurrent.Future

class Table private[db] ( name: String,
             ds: DataSource,
             freeBlobsEarly: Boolean,
             columns: String = "*")

  extends View(
    name,
    ds,
    freeBlobsEarly,
    columns) {

  def setNextIdToMaxIdPlusOne(): Transaction[Unit] = {
    maxId().map(max => setNextId(max + 1))
  }

  def setNextId(next: Long): Transaction[Boolean] = { context =>
    Future {

      val ps = context.conn.createStatement()
      try {
        ps.execute(s"ALTER TABLE ${name} ALTER COLUMN id RESTART WITH ${next};")
      } finally ps.close()

    }(context.ec)
  }

  @throws[DbOptimisticLockingException]("if the row has been updated after you read it")
  def update(values: Map[String, Any], where: Where, updateVersionCol: Boolean = false): Transaction[Unit] = {

    val params = values.keys.map(k => s"$k = ?").mkString(",")

    val versionSql = if (updateVersionCol) ", version = version + 1" else ""
    val sql = s"UPDATE $name SET $params $versionSql ${where.sql}"

    prepareStatement(sql, values.values.toSeq ++ where.params).map { ps =>

      try {
        val numRows = ps.executeUpdate()
        if (updateVersionCol && numRows == 0) throw new DbOptimisticLockingException(s"No rows were updated, optimistic lock clash? ${name}:${values}")
      } finally {
        ps.close()
      }
    }
  }

  @throws[DbOptimisticLockingException]("if the row has been updated after you read it")
  def updateRow(values: Map[String, Any]): Transaction[Row] =  {

    val minusId = values - id

    val minusVersion = minusId.filterNot {
      case (n, _) => version.equalsIgnoreCase(n)
    }

    val usingVersion = minusId.size == minusVersion.size + 1

    if (minusId.size - minusVersion.size > 1) {
      throw DbException(s"There are multiple 'version' fields in ${values}, cannot update ${name}")
    }

    if (usingVersion) {
      update(minusVersion, where(id -> values(id)) and where("version" -> values(version)), true)
    } else {
      update(minusVersion, where(id -> values(id)))
    }
    apply(values(id).asInstanceOf[Number].longValue())
  }

  /**
    * If an id is provided it will overwrite the generated identity
    * Either remove the id for genuine inserts or use 'persist' for
    * the best of both worlds.
    *
    * @param values
    * @return
    */
  def insert(values: Map[String, Any]): Transaction[Row] = {

    val names = values.keys.mkString(",")
    val params = (0 until values.keys.size).map(x => "?").mkString(",")

    val sql = s"INSERT INTO ${name} (${names}) VALUES ( ${params})"
    prepareStatement(sql, values.values.toSeq, Some(Statement.RETURN_GENERATED_KEYS)).flatMap { ps =>
      try {
        ps.executeUpdate() // run the query
        val ks = ps.getGeneratedKeys
        ks.next
        get(ks.getLong(1)).map(_.getOrElse(DbError(s"Could not retrieve generated id of row just written to ${name}")))
      } finally {
        ps.close()
      }
    }
  }

  /**
    * Use persist to either update or insert.
    *
    * If id exists in the Map and is not 0 then it's considered an update
    * If id doesn't exist in the map or in the special case where it's 0 to
    * facilitate case classes with an id default value of 0 - it's considered an insert.
    *
    * @example
    *          case class MyRecord(name : String, id: Int = 0)
    *          val row = table.persist(MyRecord("Tony"))
    *          val tony = MyRecord(row)
    *          assert(tony.name == "Tony")
    *          assert(tony.id != 0)
    *          val updatedRow = table.persist(tony.copy(name = "Karl")
    *          val karl = MyRecord(updatedRow)
    *          assert(tony.id == karl.id)
    *
    * @param values
    * @return
    */
  def persist(values: Map[String, Any]): Transaction[Row] = {

    values.partition(kv => id.equalsIgnoreCase(kv._1)) match {
      case (mapWithId, rest) if(mapWithId.isEmpty)       => insert(rest)
      case (mapWithId, rest) if(mapWithId.head._2 == 0l) => insert(rest)
      case _                                             => updateRow(values)
    }
  }

  def delete(where: Where): Transaction[Int] = {

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
  def update(values: String, filter: String): Transaction[Int] = { context =>
    Future {

      val st = context.conn.createStatement() // statement objects can be reused with
      try {

        val sql = s"UPDATE ${name} SET ${values} WHERE ${filter}"
        st.executeUpdate(sql); // run the query

      } finally {
        st.close()
      }
    }(context.ec)
  }

  def insert(values: Any*): Transaction[Int] = {

    val params = (0 until values.size).map(x => "?").mkString(",")
    val sql = s"INSERT INTO ${name} VALUES ( ${params})"
    prepareStatement(sql, values.toSeq, Some(Statement.RETURN_GENERATED_KEYS)) map { ps =>
      try {
        ps.executeUpdate() // run the query
      } finally {
        ps.close()
      }
    }
  }

}
