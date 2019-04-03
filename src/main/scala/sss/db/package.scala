package sss

import java.io.{ByteArrayInputStream, InputStream}
import java.math.BigDecimal
import java.sql.{Blob, Connection}
import java.util.regex.Pattern

import javax.sql.DataSource
import sss.ancillary.FutureOps._
import sss.ancillary.Logging
import sss.db.NullOrder.NullOrder
import sss.db.TxIsolationLevel.TxIsolationLevel

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/**
  * @author alan
  */
package object db extends Logging {

  object DbException {
    def apply(msg: String) = throw new DbException(msg)
  }

  object DbError {
    def apply(msg: String) = throw new DbError(msg)
  }

  class DbOptimisticLockingException(msg: String) extends RuntimeException(msg)

  class DbException(msg: String) extends RuntimeException(msg)

  class DbError(msg: String) extends Error(msg)

  type QueryResults[A] = IndexedSeq[A]
  type Rows = QueryResults[Row]


  type SimpleColumnTypes =
    String with
      Long with
      Short with
      Integer with
      Int with
      Float with
      Boolean with
      BigDecimal with
      Byte with
      Double with
      collection.mutable.WrappedArray[Byte] with
      Array[Byte] with
      java.sql.Date with
      java.sql.Time with
      java.sql.Timestamp with
      java.sql.Clob with
      java.sql.Blob with
      java.sql.Array with
      java.sql.Ref with
      java.sql.Struct with
      InputStream

  type ColumnTypes = SimpleColumnTypes with Option[SimpleColumnTypes]


  object TxIsolationLevel extends Enumeration {
    type TxIsolationLevel = Value
    val NONE = Value(Connection.TRANSACTION_NONE)
    val READ_COMMITTED = Value(Connection.TRANSACTION_READ_COMMITTED)
    val READ_UNCOMMITTED = Value(Connection.TRANSACTION_READ_UNCOMMITTED)
    val REPEATABLE_READ = Value(Connection.TRANSACTION_REPEATABLE_READ)
    val SERIALIZABLE = Value(Connection.TRANSACTION_SERIALIZABLE)
  }

  class RunContext(aDs: DataSource,
                   anEc: ExecutionContext,
                   val isolationLevel: Option[TxIsolationLevel] = None
                  ) {
    implicit val ds: DataSource = aDs
    implicit val ec: ExecutionContext = anEc
  }

  implicit class RunOp[T](val t: FutureTx[T]) extends AnyVal {
    def run(implicit runContext: RunContext): Future[T] = {

      val c = runContext.ds.getConnection
      runContext.isolationLevel.foreach(l => c.setTransactionIsolation(l.id))

      Try {
        t(TransactionContext(c, runContext.ec))
      } match {
        case Failure(e) =>

          try c.rollback()
          finally c.close()

          Future.failed(e)

        case Success(result) =>
          import runContext.ec
          println(s"commit a!! $result")
          result map { r =>
            try {
              c.commit()
              println(s"commit!! $r")
              r
            } finally c.close()
          } recoverWith { case e =>
            try {
              c.rollback()
              Future.failed[T](e)
            } finally c.close()
          }

      }
    }

    def runRollback(implicit runContext: RunContext): Future[T] = {
      val c = runContext.ds.getConnection
      runContext.isolationLevel.foreach(l => c.setTransactionIsolation(l.id))

      Try {
        t(TransactionContext(c, runContext.ec))
      } match {
        case Failure(e) =>

          try c.rollback()
          finally c.close()

          Future.failed(e)

        case Success(result) =>
          import runContext.ec
          result andThen { case _ =>
            try {
              c.rollback()
            } finally c.close()
          }
      }
    }
  }

  implicit class RunSyncOp[T](val t: FutureTx[T]) extends AnyVal {

    def runSync(implicit d: DataSource): Try[T] = {
      runSync(d.getConnection)
    }

    def runSync(c: Connection): Try[T] = {

      import scala.concurrent.duration._
      t(TransactionContext(c, ExecutionContextHelper.synchronousExecutionContext)).toTry(1.second) match {
        case o@Failure(_) =>
          //TODO Why bury these failures?
          Try(c.rollback()) recover { case e => log.warn(e.toString) }
          Try(c.close()) recover { case e => log.warn(e.toString) }
          o

        case o@Success(result) =>
          //TODO Why bury these failures?
          Try(c.commit()) recover { case e => log.warn(e.toString) }
          Try(c.close()) recover { case e => log.warn(e.toString) }
          o

      }
    }

    def runSyncUnSafe(implicit d: DataSource): T = runSync.get

  }

  /*case class FutTxSeq[A](value: FutureTx[Seq[A]]) {

    def map[B](f: A => B): FutTxSeq[B] =
      FutTxSeq(value.map(optA => optA.map(f)))

    def flatMap[B](f: A => FutTxSeq[B]): FutTxSeq[B] =
      FutTxSeq(
        value.flatMap { seqA: Seq[A] =>
          val s1: Seq[FutureTx[Seq[B]]] = seqA.map(f(_).value)
          val s2: FutureTx[Seq[Seq[B]]] = FutureTx.sequence(s1)
          s2 map (_.flatten)
        }
      )

  }

  case class FutTxOpt[A](value: FutureTx[Option[A]]) {

    def map[B](f: A => B): FutTxOpt[B] =
      FutTxOpt(value.map(optA => optA.map(f)))

    def flatMap[B](f: A => FutTxOpt[B]): FutTxOpt[B] =
      FutTxOpt(value.flatMap({
        case Some(a) => f(a).value
        case None => FutureTx.unit(None)
      }))
  }*/

  implicit class SqlHelper(val sc: StringContext) extends AnyVal {
    def ps(args: Any*): (String, Seq[Any]) = {
      (sc.parts.mkString("?"), args)
    }
  }

  implicit def toMap(r: Row): Map[String, _] = r.asMap

  sealed case class LimitParams(page: Int, start: Option[Long] = None)

  type Limit = Option[LimitParams]

  object NullOrder extends Enumeration {
    type NullOrder = Value
    val NullsFirst = Value("NULLS FIRST")
    val NullsLast = Value("NULLS LAST")
  }

  trait OrderBy {
    val regex = "^[a-zA-Z_][a-zA-Z0-9_]*$"
    val colName: String
    val pattern: Pattern = Pattern.compile(regex)
    require(pattern.matcher(colName).matches(), s"Column name must conform to pattern $regex")
  }

  object OrderBys {
    def apply(start: Int, pageSize: Int, orderBys: OrderBy*): OrderBys =
      OrderBys(orderBys.toSeq, Some(LimitParams(pageSize, Some(start))))

    def apply(pageSize: Int, orderBys: OrderBy*): OrderBys = OrderBys(orderBys.toSeq, Some(LimitParams(pageSize)))

    def apply(orderBys: OrderBy*): OrderBys = OrderBys(orderBys.toSeq, None)
  }

  sealed case class OrderBys(orderBys: Seq[OrderBy] = Seq.empty, limit: Limit = None) {
    def limit(start: Long, limit: Int): OrderBys = OrderBys(orderBys, Some(LimitParams(limit, Some(start))))

    def limit(limit: Int): OrderBys = OrderBys(orderBys, Some(LimitParams(limit)))

    private[db] def sql: String = {
      orderByClausesToString(orderBys) + (limit match {
        case Some(LimitParams(lim, Some(start))) => s" LIMIT $start, $lim"
        case Some(LimitParams(lim, None)) => s" LIMIT $lim"
        case None => ""
      })
    }

    private def orderByClausesToString(orderClauses: Seq[OrderBy]): String = {
      if (orderClauses.nonEmpty)
        " ORDER BY " + orderClauses.map {
          case OrderDesc(col, no) => s"$col DESC $no"
          case OrderAsc(col, no) => s"$col ASC $no"
        }.mkString(",")
      else ""
    }
  }

  sealed case class OrderDesc(colName: String, nullOrder: NullOrder = NullOrder.NullsLast) extends OrderBy

  sealed case class OrderAsc(colName: String, nullOrder: NullOrder = NullOrder.NullsLast) extends OrderBy

  sealed class Where private[db](
                                  private[db] val clause: String,
                                  private[db] val params: Seq[Any] = Seq.empty,
                                  private[db] val orderBys: OrderBys = OrderBys()) {
    private def copy(clause: String = this.clause,
                     params: Seq[Any] = this.params,
                     orderBys: OrderBys = this.orderBys): Where =
      new Where(clause, params, orderBys)

    def apply(prms: Any*): Where = copy(params = prms)

    def and(w: Where): Where = {
      val newClause = if (clause.nonEmpty && w.clause.nonEmpty) clause + " AND " + w.clause
      else clause + w.clause
      copy(clause = newClause, params = params ++ w.params)
    }

    def notIn(params: Set[_]): Where = in(params, true)

    def in(params: Set[_], neg: Boolean = false): Where = {
      val str = Seq.fill(params.size)("?") mkString ","
      val isNot = if (neg) " NOT" else ""
      val newClause = s"$clause$isNot IN ($str)"
      copy(newClause, this.params ++ params)
    }

    def using(prms: Any*): Where = apply(prms: _*)

    def orderAsc(colsAsc: String*): Where = copy(orderBys = OrderBys(colsAsc map (OrderAsc(_)), this.orderBys.limit))

    def orderDesc(colsDesc: String*): Where = copy(orderBys = OrderBys(colsDesc map (OrderDesc(_)), this.orderBys.limit))

    def orderBy(orderBys: OrderBys): Where = copy(orderBys = orderBys)

    def orderBy(orderBys: OrderBy*): Where = copy(orderBys = OrderBys(orderBys, this.orderBys.limit))

    def limit(start: Long, page: Int): Where = copy(orderBys = orderBys.limit(start, page))

    def limit(page: Int): Where = copy(orderBys = orderBys.limit(page))

    private[db] def sql: String = {
      val where = if (!clause.isEmpty) s" WHERE $clause" else ""
      where + orderBys.sql
    }
  }

  implicit def toWhere(orderBy: OrderBy): Where = new Where("", Seq.empty, OrderBys(orderBy))

  def where(): Where = new Where("")

  def where(sqlParams: (String, Seq[Any])): Where = new Where(sqlParams._1, sqlParams._2)

  def where(sql: String, params: Any*): Where = new Where(sql, params.toSeq)

  def where(tuples: (String, Any)*): Where = where(
    tuples.foldLeft[(String, Seq[Any])](("", Seq()))((acc, e) =>
      if (acc._1.isEmpty) (s"${
        e._1
      } = ?", acc._2 :+ e._2)
      else (s"${
        acc._1
      }  AND ${
        e._1
      } = ?", acc._2 :+ e._2)
    )
  )

  import scala.reflect.runtime.universe._

  implicit class Row(val asMap: Map[String, _]) {

    override def equals(o: Any) = o match {
      case that: Row => that.asMap.equals(asMap)
      case x => false
    }

    def id: Long = apply[Long]("id")

    override def hashCode = asMap.hashCode

    def get(col: String) = asMap(col.toLowerCase)

    def apply[T >: ColumnTypes : TypeTag](col: String): T = {

      val rawVal = asMap(col.toLowerCase)
      val massaged = if (typeOf[T] <:< typeOf[Option[_]] && rawVal == null) {
        None
      } else if (typeOf[T] == typeOf[Array[Byte]] && rawVal.isInstanceOf[Blob]) {
        blobToBytes(rawVal.asInstanceOf[Blob])
      } else if (typeOf[T] == typeOf[mutable.WrappedArray[Byte]] && rawVal.isInstanceOf[Blob]) {
        blobToWrappedBytes(rawVal.asInstanceOf[Blob])
      } else if (typeOf[T] == typeOf[mutable.WrappedArray[Byte]] && rawVal.isInstanceOf[Array[Byte]]) {
        new mutable.WrappedArray.ofByte(rawVal.asInstanceOf[Array[Byte]])
      } else if (typeOf[T] == typeOf[InputStream] && rawVal.isInstanceOf[Blob]) {
        blobToStream(rawVal.asInstanceOf[Blob])
      } else if (typeOf[T] == typeOf[Byte] && rawVal.isInstanceOf[Array[Byte]]) {
        val aryByte = rawVal.asInstanceOf[Array[Byte]]
        assert(aryByte.length == 1)
        aryByte(0)
      } else if (typeOf[T] == typeOf[InputStream] && rawVal.isInstanceOf[Array[Byte]]) {
        val aryByte = rawVal.asInstanceOf[Array[Byte]]
        new ByteArrayInputStream(aryByte)
      } else if (typeOf[T] =:= typeOf[ColumnTypes]) {
        //in the case where NO parameter type is passed it defaults to ColumnTypes
        // and ColumnTypes will match typeOf[Option[_]] so we must prevent that here
        rawVal
      } else if (typeOf[T] <:< typeOf[Option[_]])
        Some(rawVal)
      else rawVal

      massaged.asInstanceOf[T]
    }

    private def blobToStream(jDBCBlobClient: Blob): InputStream = jDBCBlobClient.getBinaryStream

    private def blobToBytes(jDBCBlobClient: Blob): Array[Byte] = jDBCBlobClient.getBytes(1, jDBCBlobClient.length.toInt)

    private def blobToWrappedBytes(jDBCBlobClient: Blob): mutable.WrappedArray[Byte] = jDBCBlobClient.getBytes(1, jDBCBlobClient.length.toInt)

    override def toString: String = {
      asMap.foldLeft("") { case (a, (k, v)) => a + s" Key:$k, Value: $v" }
    }
  }

}