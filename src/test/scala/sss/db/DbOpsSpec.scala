package sss.db

import org.scalatest.DoNotDiscover
import sss.db.ops.DbOps._

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

@DoNotDiscover
class DbOpsSpec extends DbSpecSetup {

  // ---- DbRunOps ----

  "DbRunOps" should "run sync via implicit Db" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext
    val row = fixture.table.persist(Map("strId" -> "s", "createTime" -> 0L, "intVal" -> 1)).dbRunSyncGet
    assert(row.int("intVal") == 1)
  }

  it should "return Failure via dbRunSync on error" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext
    val result = FutureTx.failed[Row](new RuntimeException("boom")).dbRunSync
    assert(result.isFailure)
  }

  it should "run async via dbRun" in {
    implicit val db = fixture.dbUnderTest
    import db.asyncRunContext
    val future = fixture.table.persist(Map("strId" -> "async", "createTime" -> 0L, "intVal" -> 99)).dbRun
    val row = Await.result(future, 5.seconds)
    assert(row.int("intVal") == 99)
  }

  // ---- FutureTxOps.recover ----

  "FutureTxOps" should "recover from failure with a value (using ec)" in {
    val db = fixture.dbUnderTest
    import db.syncRunContext
    implicit val ec: ExecutionContext = db.asyncRunContext.ec

    val op: FutureTx[Int] = FutureTx.failed[Int](new RuntimeException("oops"))
      .recover { case _: RuntimeException => 42 }

    assert(op.runSyncAndGet == 42)
  }

  it should "not invoke recover when successful (using ec)" in {
    val db = fixture.dbUnderTest
    import db.syncRunContext
    implicit val ec: ExecutionContext = db.asyncRunContext.ec

    val op: FutureTx[Int] = FutureTx.unit(7)
      .recover { case _: RuntimeException => 42 }

    assert(op.runSyncAndGet == 7)
  }

  it should "recoverWith from failure using another FutureTx (using ec)" in {
    val db = fixture.dbUnderTest
    import db.syncRunContext
    implicit val ec: ExecutionContext = db.asyncRunContext.ec

    val fallback: FutureTx[String] = FutureTx.unit("fallback")
    val op: FutureTx[String] = FutureTx.failed[String](new RuntimeException("oops"))
      .recoverWith { case _: RuntimeException => fallback }

    assert(op.runSyncAndGet == "fallback")
  }

  it should "not invoke recoverWith when successful (using ec)" in {
    val db = fixture.dbUnderTest
    import db.syncRunContext
    implicit val ec: ExecutionContext = db.asyncRunContext.ec

    val op: FutureTx[String] = FutureTx.unit("ok")
      .recoverWith { case _: RuntimeException => FutureTx.unit("fallback") }

    assert(op.runSyncAndGet == "ok")
  }

  // ---- FutureTxOps.recoverDb / recoverWithDb ----

  it should "recover from failure with a value (using implicit Db)" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext

    val op: FutureTx[Int] = FutureTx.failed[Int](new RuntimeException("oops"))
      .recoverDb { case _: RuntimeException => 99 }

    assert(op.runSyncAndGet == 99)
  }

  it should "recoverWith from failure using another FutureTx (using implicit Db)" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext

    val op: FutureTx[String] = FutureTx.failed[String](new RuntimeException("oops"))
      .recoverWithDb { case _: RuntimeException => FutureTx.unit("recovered") }

    assert(op.runSyncAndGet == "recovered")
  }

  // ---- FutureTryTxOps.unwrapTry (FutureTx[Try[T]]) ----

  "FutureTryTxOps" should "unwrap a successful FutureTx[Try[T]]" in {
    val db = fixture.dbUnderTest
    import db.syncRunContext

    val op: FutureTx[String] = FutureTx.unit(Success("hello"): Try[String]).unwrapTry
    assert(op.runSyncAndGet == "hello")
  }

  it should "fail when unwrapping a FutureTx[Failure[T]]" in {
    val db = fixture.dbUnderTest
    import db.syncRunContext
    val rte = new RuntimeException("inner failure")

    val op: FutureTx[String] = FutureTx.unit(Failure(rte): Try[String]).unwrapTry
    assert(op.runSync == Failure(rte))
  }

  // ---- FutureTxFutureTxOps.flattenFutureTx ----

  "FutureTxFutureTxOps" should "flatten a nested FutureTx[FutureTx[T]]" in {
    val db = fixture.dbUnderTest
    import db.syncRunContext
    implicit val ec: ExecutionContext = db.asyncRunContext.ec

    val inner: FutureTx[String] = FutureTx.unit("nested")
    val outer: FutureTx[FutureTx[String]] = FutureTx.unit(inner)
    val flat: FutureTx[String] = outer.flattenFutureTx(outer)
    assert(flat.runSyncAndGet == "nested")
  }

  // ---- OptFutureTxOps.unwrapOpt ----

  "OptFutureTxOps" should "succeed when unwrapping Some(FutureTx)" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext

    val op = Option(FutureTx.unit("present")).unwrapOpt
    assert(op.runSyncAndGet == "present")
  }

  it should "fail when unwrapping None" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext

    val op = (None: Option[FutureTx[String]]).unwrapOpt
    assert(op.runSync.isFailure)
  }

  // ---- EitherFutureTxOps.unwrapEither ----

  "EitherFutureTxOps" should "succeed when unwrapping Right(FutureTx)" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext

    val op = (Right(FutureTx.unit("right")): Either[String, FutureTx[String]]).unwrapEither
    assert(op.runSyncAndGet == "right")
  }

  it should "fail when unwrapping Left(Exception)" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext
    val err = new RuntimeException("left error")

    val op = (Left(err): Either[RuntimeException, FutureTx[String]]).unwrapEither
    assert(op.runSync == Failure(err))
  }

  it should "fail with RuntimeException when unwrapping Left(non-Exception)" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext

    val op = (Left("some error string"): Either[String, FutureTx[String]]).unwrapEither
    assert(op.runSync.isFailure)
  }

  // ---- TryFutureTxOps.unwrapTry ----

  "TryFutureTxOps" should "succeed when unwrapping Success(FutureTx)" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext

    val op = (Success(FutureTx.unit("ok")): Try[FutureTx[String]]).unwrapTry
    assert(op.runSyncAndGet == "ok")
  }

  it should "fail when unwrapping Failure" in {
    implicit val db = fixture.dbUnderTest
    import db.syncRunContext
    val rte = new RuntimeException("try failure")

    val op = (Failure(rte): Try[FutureTx[String]]).unwrapTry
    assert(op.runSync == Failure(rte))
  }
}
