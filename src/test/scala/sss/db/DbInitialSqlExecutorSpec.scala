package sss.db

import org.scalatest.DoNotDiscover

import java.sql.SQLException
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

@DoNotDiscover
class DbInitialSqlExecutorSpec extends DbSpecSetup {

  private def config(
    deleteSql: Option[Seq[String]] = None,
    createSql: Option[Seq[String]] = None
  ): DbConfig = new DbConfig {
    val freeBlobsEarly: Boolean = false
    val useShutdownHook: Boolean = false
    val viewCachesSize: Int = 10
    val deleteSqlOpt: Option[java.lang.Iterable[String]] = deleteSql.map(_.asJava)
    val createSqlOpt: Option[java.lang.Iterable[String]] = createSql.map(_.asJava)
  }

  "DbInitialSqlExecutor" should "execute deleteSql successfully without throwing" in {
    implicit val src: SyncRunContext = fixture.dbUnderTest.syncRunContext
    val called = collection.mutable.Buffer[String]()
    val executeSql: String => FutureTx[Int] = sql => { called += sql; FutureTx.unit(1) }

    DbInitialSqlExecutor(config(deleteSql = Some(Seq("DELETE FROM test WHERE 1=0"))), executeSql)

    assert(called.toSeq == Seq("DELETE FROM test WHERE 1=0"))
  }

  it should "warn but not throw on deleteSql SQLException" in {
    implicit val src: SyncRunContext = fixture.dbUnderTest.syncRunContext
    val sqle = new SQLException("table not found")
    val executeSql: String => FutureTx[Int] = _ => FutureTx.failed[Int](sqle)

    // should not throw
    DbInitialSqlExecutor(config(deleteSql = Some(Seq("DROP TABLE nonexistent"))), executeSql)
  }

  it should "re-throw non-SQLException from deleteSql" in {
    implicit val src: SyncRunContext = fixture.dbUnderTest.syncRunContext
    val rte = new RuntimeException("unexpected failure")
    val executeSql: String => FutureTx[Int] = _ => FutureTx.failed[Int](rte)

    assertThrows[RuntimeException] {
      DbInitialSqlExecutor(config(deleteSql = Some(Seq("DROP TABLE nonexistent"))), executeSql)
    }
  }

  it should "execute createSql successfully without throwing" in {
    implicit val src: SyncRunContext = fixture.dbUnderTest.syncRunContext
    val called = collection.mutable.Buffer[String]()
    val executeSql: String => FutureTx[Int] = sql => { called += sql; FutureTx.unit(0) }

    DbInitialSqlExecutor(config(createSql = Some(Seq("CREATE TABLE IF NOT EXISTS tmp_x (id INT)"))), executeSql)

    assert(called.toSeq == Seq("CREATE TABLE IF NOT EXISTS tmp_x (id INT)"))
  }

  it should "warn but not throw on createSql SQLException" in {
    implicit val src: SyncRunContext = fixture.dbUnderTest.syncRunContext
    val sqle = new SQLException("already exists")
    val executeSql: String => FutureTx[Int] = _ => FutureTx.failed[Int](sqle)

    // should not throw
    DbInitialSqlExecutor(config(createSql = Some(Seq("CREATE TABLE already_there (id INT)"))), executeSql)
  }

  it should "re-throw non-SQLException from createSql" in {
    implicit val src: SyncRunContext = fixture.dbUnderTest.syncRunContext
    val rte = new RuntimeException("driver down")
    val executeSql: String => FutureTx[Int] = _ => FutureTx.failed[Int](rte)

    assertThrows[RuntimeException] {
      DbInitialSqlExecutor(config(createSql = Some(Seq("CREATE TABLE x (id INT)"))), executeSql)
    }
  }

  it should "skip empty strings in deleteSql" in {
    implicit val src: SyncRunContext = fixture.dbUnderTest.syncRunContext
    val called = collection.mutable.Buffer[String]()
    val executeSql: String => FutureTx[Int] = sql => { called += sql; FutureTx.unit(0) }

    DbInitialSqlExecutor(config(deleteSql = Some(Seq("", "DELETE FROM test WHERE 1=0", ""))), executeSql)

    assert(called.toSeq == Seq("DELETE FROM test WHERE 1=0"))
  }

  it should "do nothing when deleteSqlOpt and createSqlOpt are None" in {
    implicit val src: SyncRunContext = fixture.dbUnderTest.syncRunContext
    var called = false
    val executeSql: String => FutureTx[Int] = _ => { called = true; FutureTx.unit(0) }

    DbInitialSqlExecutor(config(), executeSql)

    assert(!called)
  }
}
