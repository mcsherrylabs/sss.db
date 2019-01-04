package sss.db

import java.sql.Statement

import javax.sql.DataSource

class Table private[db] ( name: String,
             ds: DataSource,
             freeBlobsEarly: Boolean,
             columns: String = "*")

  extends View(
    name,
    ds,
    freeBlobsEarly,
    columns) {

  def setNextIdToMaxIdPlusOne(): Unit = inTransaction {
    setNextId(maxId() + 1)
  }

  def setNextId(next: Long): Unit = {

    val ps = conn.createStatement()
    try {
      ps.execute(s"ALTER TABLE ${name} ALTER COLUMN id RESTART WITH ${next};")
    } finally ps.close

  }

  @throws[DbOptimisticLockingException]("if the row has been updated after you read it")
  def update(values: Map[String, Any]): Row = inTransaction {

    val minusVersion = values.filterNot { case (n: String, v) => version.equalsIgnoreCase(n) }

    val usingVersion = values.size == minusVersion.size + 1

    if (values.size - minusVersion.size > 1) {
      throw DbException(s"There are multiple 'version' fields in ${values}, cannot update ${name}")
    }

    val params = minusVersion.map { case (k: String, v) => s"$k=?" }.mkString(",")

    val (sql, p) = if (usingVersion) {
      (s"UPDATE ${name} SET ${params}, version = version + 1 WHERE id = ? AND version = ?", minusVersion.values.toSeq :+ values(id) :+ values(version))
    } else {
      (s"UPDATE ${name} SET ${params} WHERE id = ?", minusVersion.values.toSeq :+ values(id))
    }

    val ps = prepareStatement(sql, p)

    try {
      val numRows = ps.executeUpdate()
      if (usingVersion && numRows == 0) throw new DbOptimisticLockingException(s"No rows were updated, optimistic lock clash? ${name}:${values}")
      apply(values(id).asInstanceOf[Number].longValue())
    } finally {
      ps.close
    }
  }

  /**
    * If an id is provided it will overwrite the generated identity
    * Either remove the id for genuine inserts or use 'persist' for
    * the best of both worlds.
    *
    * @param values
    * @return
    */
  def insert(values: Map[String, Any]): Row = inTransaction {

    val names = values.keys.mkString(",")
    val params = (0 until values.keys.size).map(x => "?").mkString(",")

    val sql = s"INSERT INTO ${name} (${names}) VALUES ( ${params})"
    val ps = prepareStatement(sql, values.values.toSeq, Some(Statement.RETURN_GENERATED_KEYS))
    try {
      ps.executeUpdate() // run the query
      val ks = ps.getGeneratedKeys
      ks.next
      get(ks.getLong(1)).getOrElse(DbError(s"Could not retrieve generated id of row just written to ${name}"))
    } finally {
      ps.close
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
  def persist(values: Map[String, Any]): Row = inTransaction {

    values.partition(kv => id.equalsIgnoreCase(kv._1)) match {
      case (mapWithId, rest) if(mapWithId.isEmpty)       => insert(rest)
      case (mapWithId, rest) if(mapWithId.head._2 == 0l) => insert(rest)
      case _                                             => update(values)
    }
  }

  def delete(where: Where): Int = tx[Int] {

    val ps = prepareStatement(s"DELETE FROM $name ${where.sql}", where.params)
    try {
      ps.executeUpdate(); // run the query
    } finally {
      ps.close()
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
  def update(values: String, filter: String): Int = tx {

    val st = conn.createStatement(); // statement objects can be reused with
    try {

      val sql = s"UPDATE ${name} SET ${values} WHERE ${filter}"
      st.executeUpdate(sql); // run the query

    } finally {
      st.close
    }
  }

  def insert(values: Any*): Int = tx {

    val params = (0 until values.size).map(x => "?").mkString(",")
    val sql = s"INSERT INTO ${name} VALUES ( ${params})"
    val ps = prepareStatement(sql, values.toSeq, Some(Statement.RETURN_GENERATED_KEYS))
    try {
      ps.executeUpdate(); // run the query
    } finally {
      ps.close
    }
  }

}
