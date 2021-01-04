package sss.db

import java.util.Date

import org.scalatest.DoNotDiscover
import sss.db

import concurrent.{Await, Future}
import concurrent.duration._
import language.postfixOps
import concurrent.ExecutionContext.Implicits.global

@DoNotDiscover
class ParallelThreadSupportSpec extends DbSpecSetup {

  def parallelEffect(j: Int):FutureTx[Boolean] = for {
      c <- fixture.table.count
      i <- fixture.table.insert(j, s"strId $j", new Date().getTime, j)
      _ = assert(i === 1, "should only insert 1 row")
      nc <- fixture.table.count
      //_ = println(s"$i $nc $c")
  } yield nc >= c + 1

  it should "support consistent parallel threaded access" in {

    val db = fixture.dbUnderTest
    import db.runContext
    import db.runContext._


    val effects = for {
      i <- 0 until 100
    } yield parallelEffect(i)

    effects.map(_.run).foreach { effect =>
      assert(Await.result(effect, 10 seconds) === true)
    }

    assert(Await.result(FutureTx.sequence(effects).run, 10.seconds).forall(_ == true), "Run using sequence")

    assert(fixture.table.count.runSyncAndGet === 200, "Should only be 200 rows inserted")
  }



}
