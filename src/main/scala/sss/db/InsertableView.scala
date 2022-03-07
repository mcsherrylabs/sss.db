package sss.db

import java.sql.Statement

class InsertableView private[db](name: String,
                                 baseWhere: Where,
                                 runContext: RunContext,
                                 freeBlobsEarly: Boolean,
                                 columns: String = "*")

  extends UpdatableView(
    name,
    baseWhere,
    runContext,
    freeBlobsEarly,
    columns) {

  /**
    * If an id is provided it will overwrite the generated identity
    * Either remove the id for genuine inserts or use 'persist' for
    * the best of both worlds.
    *
    * @param values
    * @return
    */
  def insert(values: Map[String, Any]): FutureTx[Row] = {

    val (keys, vals) = values.splitKeyValues
    val names = keys.mkString(",")
    val params = keys.indices.map(x => "?").mkString(",")

    val sql = s"INSERT INTO ${name} (${names}) VALUES ( ${params})"
    prepareStatement(sql, vals, Some(Statement.RETURN_GENERATED_KEYS)).flatMap { ps =>
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
    * case class MyRecord(name : String, id: Int = 0)
    * val row = table.persist(MyRecord("Tony"))
    * val tony = MyRecord(row)
    * assert(tony.name == "Tony")
    * assert(tony.id != 0)
    * val updatedRow = table.persist(tony.copy(name = "Karl")
    * val karl = MyRecord(updatedRow)
    * assert(tony.id == karl.id)
    * @param values
    * @return
    */
  def persist(values: Map[String, Any]): FutureTx[Row] = {

    values.partition(kv => id.equalsIgnoreCase(kv._1)) match {
      case (mapWithId, rest) if mapWithId.isEmpty       => insert(rest)
      case (mapWithId, rest) if mapWithId.head._2 == 0L => insert(rest)
      case _                                            => updateRow(values)
    }
  }

  def insert(values: Any*): FutureTx[Int] = {

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

  /**
  Use when there is no identity column to insert a row
    *
    * @param values
    * @return
    */
  def insertNoIdentity(values: Map[String, Any]): FutureTx[Int] = {

    val (keys, vals) = values.splitKeyValues
    val names = keys.mkString(",")
    val params = keys.indices.map(x => "?").mkString(",")

    val sql = s"INSERT INTO ${name} (${names}) VALUES ( ${params})"
    prepareStatement(sql, vals).map { ps =>
      try {
        ps.executeUpdate() // run the query
      } finally {
        ps.close()
      }
    }
  }

}
