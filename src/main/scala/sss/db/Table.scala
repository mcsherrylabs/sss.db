package sss.db

class Table private[db] (name: String,
                         runContext: RunContext,
                         freeBlobsEarly: Boolean,
                         columns: String = "*")

  extends InsertableView(
    name,
    where(),
    runContext,
    freeBlobsEarly,
    columns) {

  @deprecated("This causes an implicit commit in some databases")
  def setNextIdToMaxIdPlusOne(): FutureTx[Boolean] = {
    maxId().flatMap(max => setNextId(max + 1))
  }

  /**
    *
    * @param next
    * @return
    */
  @deprecated("This causes an implicit commit in some databases")
  def setNextId(next: Long): FutureTx[Boolean] = { context =>
    LoggingFuture {

      val ps = context.conn.createStatement()
      try {
        ps.execute(s"ALTER TABLE ${name} ALTER COLUMN id RESTART WITH ${next};")
      } finally ps.close()

    }(context.ec)
  }
}
