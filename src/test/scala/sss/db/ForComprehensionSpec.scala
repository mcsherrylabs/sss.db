package sss.db

import org.scalatest.DoNotDiscover

import scala.util.{Success, Try}

@DoNotDiscover
class ForComprehensionSpec extends DbSpecSetup {


  "A paged view " should "support a paged stream generator " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    import db.runContext.executor

    for {
      x <- 0 until 100
      _ = db.table("testForComp").insert(x, x+1, "strId" + x).runSyncAndGet

    } yield ()

    var rs: LazyList[Row] = fixture.dbUnderTest.table("testForComp").toPaged(2).toStream
    val rows = for {
      x <- (0 until 10)
      _ = fixture.dbUnderTest.table("testForComp").insert(x, x+1, "strId" + x)
      rs = (fixture.dbUnderTest.table("testForComp").toPaged(2)).iterator
      r        <- rs
      if(r[Int](idCol) == 1)
    } yield(r)

    0 until 50 foreach { i =>
      //println(s"$i ${rs.headOption}")
      rs = rs.tail
    }

    assert(rows.size === 10, "Should be 10 rows!")

  }

  def createAndFillTenTables: Seq[String] = {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    import db.runContext.executor

    //create 10 tables
    val tableNames = for {
      index <- 0 until 10
      blockSql = s"CREATE TABLE IF NOT EXISTS block_$index " +
        s"(id INT GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1), " +
        s"$statusCol VARCHAR(50), " +
        s"PRIMARY KEY(id)) "

      _ = fixture.dbUnderTest.executeSql(blockSql).runSyncAndGet
    } yield s"block_$index"

    //put 10 lines in each table = 100 rows
    for {
      name <- tableNames
      table = fixture.dbUnderTest.table(name)
      x <- (0 until 10)
      _ = table.insert(Map(statusCol -> s"Something $name")).runSyncAndGet
    } yield()

    tableNames
  }

  it should " support paged iterator generator " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    import db.runContext.executor

    val tableNames = createAndFillTenTables
    // go through each table in a paged manner and find all the rows
    // where the idCol is 1 or 9, should be 20 of them.
    val allRows = for {
      name <- tableNames
      table = fixture.dbUnderTest.table(name)
      row <- table.toPaged(3).toIterator
      if row[Int](idCol) == 1 || row[Int](idCol) == 9
    } yield row

    //allRows.foreach(println)
    assert(allRows.size === 20)

    val checked = for {
      r <- allRows
      if r[Int](idCol) != 1 && r[Int](idCol) != 9
    } yield r
    assert(checked.size === 0)
  }

  it should " support rollback in transactions using Try " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    import db.runContext.executor
    val x = 1000
    val t = fixture.dbUnderTest.table("testForComp")



    def badTx(r: Int):FutureTx[Int] = {
      assert(r == x + 1)
      throw new RuntimeException("Woh!")
    }

    def goodTx= t.insert(x, x + 1, "strId" + x)

    val result =
      (for {
        v1 <- goodTx
        v2 <- badTx(v1)
      } yield v2).runSync

    assert(result.isFailure, "Should have thrown exception in badTx")
    assert(t.find(where(idCol -> x)).runSyncAndGet.isEmpty, "tx rollback should have prevented row write")

  }


  it should " support nested generator (thru flatMap) " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    import db.runContext.executor

    val tableNames = createAndFillTenTables

    val r1: Seq[Row] = for {
      name <- tableNames
      table = fixture.dbUnderTest.table(name)
      row <- table.map(identity).runSyncAndGet
    } yield row

    assert(r1.size === 100)

    val allRowsFiltered = for {
      name <- tableNames
      table = fixture.dbUnderTest.table(name).map(identity).runSyncAndGet
      row <- table
      row2 <- fixture.dbUnderTest.table(name).map(identity).runSyncAndGet
      if row[Int](idCol) == row2[Int](idCol)
    } yield row2

    assert(allRowsFiltered.size === 100)
    //allRowsFiltered.foreach(println)

  }

  it should " support FutureTx nested generator (thru flatMap) " in {

    val db = fixture.dbUnderTest
    import db.runContext.ds
    import db.runContext.executor


    val tableNames = createAndFillTenTables

    val seqFutTxs2 :Seq[FutureTx[QueryResults[Row]]] = (for {
      name <- tableNames
      table = fixture.dbUnderTest.table(name)
    } yield Seq(table.map(identity), table.map(identity))).flatten

    val futSeqRows: FutureTx[Seq[QueryResults[Row]]] = FutureTx.sequence(seqFutTxs2)

    val futRows: FutureTx[Seq[Row]] = futSeqRows.map(_.flatten)

    val futRowsFilter = futRows.map(rows => rows.filter(_.int("id") <= 5))

    futRowsFilter.map {
      all =>
        assert(all.size === 100)
    }.runSyncAndGet



    //run another test, checks no interference in first test.
    val seqFutTxs :Seq[FutureTx[QueryResults[Row]]] = for {
      name <- tableNames
      table = fixture.dbUnderTest.table(name)
    } yield table.map(identity)

    val futTxSeq = FutureTx.sequence(seqFutTxs)

    futTxSeq.map { r1 =>
        assert(r1.flatten.size === 100)
    }.runSyncAndGet

  }
}