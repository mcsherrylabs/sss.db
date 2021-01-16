package sss

import sss.ancillary.FutureOps.AwaitReady
import sss.ancillary.Logging
import sss.db.IsNull.IsNull

import java.io.{ByteArrayInputStream, InputStream}
import java.math.BigDecimal
import java.sql.{Blob, Connection}
import java.util.regex.Pattern
import javax.sql.DataSource
import sss.db.NullOrder.NullOrder
import sss.db.TxIsolationLevel.TxIsolationLevel

import java.util
import java.util.Locale
import scala.collection.mutable
import scala.concurrent.duration.Duration
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
      mutable.ArraySeq[Byte] with
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

  trait RunContext {
    implicit val ds: DataSource
    implicit val executor: FutureTxExecutor
    val isolationLevel: Option[TxIsolationLevel]
  }

  class AsyncRunContext(aDs: DataSource,
                   anEc: ExecutionContext,
                   val isolationLevel: Option[TxIsolationLevel] = None,
                   anExecutor: FutureTxExecutor = FutureTxExecutor
                  ) extends RunContext {
    implicit val ds: DataSource = aDs
    implicit val ec: ExecutionContext = anEc
    implicit val executor: FutureTxExecutor = anExecutor
  }

  class SyncRunContext(aDs: DataSource,
                       val timeout: Duration,
                       val isolationLevel: Option[TxIsolationLevel] = None,
                       anExecutor: FutureTxExecutor = FutureTxExecutor
                      ) extends RunContext {
    implicit val ds: DataSource = aDs
    implicit val executor: FutureTxExecutor = anExecutor

  }

  implicit class RunOp[T](val t: FutureTx[T]) extends AnyVal {
    def run(implicit runContext: AsyncRunContext): Future[T] = {
      runContext.executor.execute(t, runContext, false)
    }

    def runRollback(implicit runContext: AsyncRunContext): Future[T] = {
      runContext.executor.execute(t, runContext, true)
    }
  }

  implicit class RunSyncOp[T](val t: FutureTx[T]) extends AnyVal {

    def runSync(implicit runContext: SyncRunContext): Try[T] = {
      runContext.executor.executeSync(
        t,
        runContext.ds,
        false,
        runContext.isolationLevel,
        runContext.timeout
      )
    }

    def runSyncRollback(implicit runContext: SyncRunContext): Try[T] = {
      runContext.executor.executeSync(
        t,
        runContext.ds,
        true,
        runContext.isolationLevel,
        runContext.timeout
      )
    }

    def runSyncAndGet(implicit runContext: SyncRunContext): T = runSync.get
    def runSyncRollbackAndGet(implicit runContext: SyncRunContext): T = runSyncRollback.get
  }

  implicit class SqlHelper(val sc: StringContext) extends AnyVal {
    def ps(args: Any*): (String, Seq[Any]) = {
      (sc.parts.mkString("?"), args)
    }
  }

  implicit def toMap(r: Row): Map[String, _] = r.asMap

  sealed case class LimitParams(page: Int, start: Option[Long] = None)

  type Limit = Option[LimitParams]

  object IsNull extends Enumeration {
    type IsNull = Value
    val Null = Value(" IS NULL")
    val NotNull = Value(" IS NOT NULL")
  }


  object NullOrder extends Enumeration {
    type NullOrder = Value
    val NullsFirst = Value("NULLS FIRST")
    val NullsLast = Value("NULLS LAST")
  }

  trait OrderBy {
    val regex = "^[a-zA-Z_][a-zA-Z0-9_]*$"
    val colName: String
    val pattern = Pattern.compile(regex)
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

    def is(p: IsNull): Where = {
      val newClause = s"$clause$p"
      copy(newClause, params)
    }

    def in(params: Set[_], neg: Boolean = false): Where = {
      val str = Seq.fill(params.size)("?") mkString(",")
      val isNot = if(neg) " NOT" else ""
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
      val where = if (clause.nonEmpty) s" WHERE $clause" else ""
      where + orderBys.sql
    }
  }

  object WhereOps {
    implicit def toWhere(orderBy: OrderBy): Where = new Where("", Seq.empty, OrderBys(orderBy))
  }


  def where(): Where = new Where("")

  def where(sqlParams: (String, Seq[Any])): Where = new Where(sqlParams._1, sqlParams._2)

  def where(sql: String, params: Any*): Where = new Where(sql, params.toSeq)


  def where(tuples: (String, Any)*): Where =
    where(
      tuples.foldLeft[(String, Seq[Any])](("", Seq()))((acc, e) => (acc._1, e._2) match {
        case ("", None) =>
          (s"${e._1} IS NULL", acc._2)
        case ("", _) =>
          (s"${e._1} = ?", acc._2 :+ e._2)
        case (_, None) =>
          (s"${acc._1} AND ${e._1} IS NULL", acc._2)
        case _ =>
          (s"${acc._1} AND ${e._1} = ?", acc._2 :+ e._2)
      })
    )


  import scala.reflect.runtime.universe._


  case class ColumnMetaInfo(name: String, `type`: Int, noNullsAllowed: Boolean)

  type ColumnsMetaInfo = Seq[ColumnMetaInfo]

  class Row(val asMap: Map[String, _]) {

    def shallowEquals(that: Row): Boolean = {
      asMap.forall {
        case (k, _: Blob | _: InputStream) =>
          that.asMap(k) match {
            case _: Blob | _ :InputStream => true
            case _ => false
          }
        case (k, v: Array[Byte]) =>
          that.asMap(k) match {
            case v2: Array[Byte] => util.Arrays.equals(v, v2)
            case _ => false
          }
        case (k, v) =>
          that.asMap(k) == v
      }
    }

    override def equals(o: Any): Boolean = o match {
      case that: Row =>
        asMap.forall {
          case (k, v: Array[Byte]) =>
            that.asMap(k) match {
              case v2: Array[Byte] => util.Arrays.equals(v, v2)
              case _ => false
            }
          case (k, v) =>
            that.asMap(k) == v
        }
      case x => false
    }

    def id: Long = long("id")

    override def hashCode = asMap.hashCode

    def get(col: String) = asMap(col.toLowerCase(Locale.ROOT))

    @deprecated("Use string(), int(), long() etc. instead.", "1.5-SNAPSHOT")
    def apply[T >: ColumnTypes : TypeTag](col: String): T = {

      val rawVal = asMap(col.toLowerCase(Locale.ROOT))
      val massaged = if (typeOf[T] <:< typeOf[Option[_]] && rawVal == null) {
        None
      } else if (typeOf[T] == typeOf[Array[Byte]] && rawVal.isInstanceOf[Blob]) {
        blobToBytes(rawVal.asInstanceOf[Blob])
      } else if (typeOf[T] == typeOf[mutable.ArraySeq[Byte]] && rawVal.isInstanceOf[Blob]) {
        blobToWrappedBytes(rawVal.asInstanceOf[Blob])
      } else if (typeOf[T] == typeOf[mutable.ArraySeq[Byte]] && rawVal.isInstanceOf[Array[Byte]]) {
        (rawVal.asInstanceOf[Array[Byte]]).to(mutable.ArraySeq)
      } else if (typeOf[T] == typeOf[InputStream] && rawVal.isInstanceOf[Blob]) {
        blobToStream(rawVal.asInstanceOf[Blob])
      } else if (typeOf[T] == typeOf[Byte] && rawVal.isInstanceOf[Array[Byte]]) {
        shimObjectToByte(rawVal)
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


    private def shimObjectToByte(o: Any): Byte = {
      val aryByte = o.asInstanceOf[Array[Byte]]
      require(aryByte.length == 1)
      aryByte(0)
    }

    def number(col: String): Number = asMap(col.toLowerCase(Locale.ROOT)).asInstanceOf[Number]

    def stringOpt(col: String): Option[String] = Option(asMap(col.toLowerCase(Locale.ROOT))).map(_.asInstanceOf[String])

    def string(col: String): String = stringOpt(col).get

    def longOpt(col: String): Option[Long] = Option(asMap(col.toLowerCase(Locale.ROOT))).map(_.asInstanceOf[Long])

    def long(col: String): Long = longOpt(col).get

    def intOpt(col: String): Option[Int] = Option(asMap(col.toLowerCase(Locale.ROOT))).map(_.asInstanceOf[Int])

    def int(col: String): Int = intOpt(col).get

    def bigDecimal(col: String): BigDecimal = bigDecimalOpt(col).get

    def bigDecimalOpt(col: String): Option[BigDecimal] = Option(asMap(col.toLowerCase(Locale.ROOT))).map(_.asInstanceOf[BigDecimal])


    def byteOpt(col: String): Option[Byte] = Option(asMap(col.toLowerCase(Locale.ROOT))).map(shimObjectToByte)

    def byte(col: String): Byte = byteOpt(col).get

    def shortOpt(col: String): Option[Short] = Option(asMap(col.toLowerCase(Locale.ROOT))).map(_.asInstanceOf[Short])

    def short(col: String): Short = asMap(col.toLowerCase(Locale.ROOT)).asInstanceOf[Short]

    def booleanOpt(col: String): Option[Boolean] = Option(asMap(col.toLowerCase(Locale.ROOT))).map(_.asInstanceOf[Boolean])

    def boolean(col: String): Boolean = asMap(col.toLowerCase(Locale.ROOT)).asInstanceOf[Boolean]

    def arrayByteOpt(col: String): Option[Array[Byte]] = Option(asMap(col.toLowerCase(Locale.ROOT))).map(_.asInstanceOf[Array[Byte]])

    def arrayByte(col: String): Array[Byte] = asMap(col.toLowerCase(Locale.ROOT)).asInstanceOf[Array[Byte]]

    def blobByteArrayOpt(col: String): Option[Array[Byte]] = Option(asMap(col.toLowerCase(Locale.ROOT)).asInstanceOf[Blob]).map(blobToBytes)

    def blobByteArray(col: String): Array[Byte] = blobToBytes(asMap(col.toLowerCase(Locale.ROOT)).asInstanceOf[Blob])

    def blobInputStreamOpt(col: String): Option[InputStream] =
      Option(asMap(col.toLowerCase(Locale.ROOT))
        .asInstanceOf[Blob])
        .map(blobToStream)

    def blobInputStream(col: String): InputStream = blobInputStreamOpt(col).get

    def blob(col: String): Blob = blobOpt(col).get

    def blobOpt(col: String): Option[Blob] = Option(asMap(col.toLowerCase(Locale.ROOT)).asInstanceOf[Blob])

    private def blobToStream(jDBCBlobClient: Blob): InputStream = jDBCBlobClient.getBinaryStream

    private def blobToBytes(jDBCBlobClient: Blob): Array[Byte] = jDBCBlobClient.getBytes(1, jDBCBlobClient.length.toInt)

    private def blobToWrappedBytes(jDBCBlobClient: Blob): mutable.ArraySeq[Byte] = jDBCBlobClient.getBytes(1, jDBCBlobClient.length.toInt).to(mutable.ArraySeq)

    override def toString: String = {
      asMap.foldLeft("") { case (a, (k, v)) => a + s" Key:$k, Value: $v" }
    }
  }

}