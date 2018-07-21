package sss.db

import java.util.Date

import concurrent.{Await, Future}
import concurrent.duration._
import language.postfixOps
import concurrent.ExecutionContext.Implicits.global

//@DoNotDiscover
class ParallelThreadSupportSpec extends DbSpecSetup {

  def parallelEffect(j: Int):Future[Boolean] = Future {
    fixture.table.tx {
      val tname = Thread.currentThread().getName
      val c = fixture.table.count
      val i = fixture.table.insert(j, s"strId $j", new Date().getTime, j)
      assert(i === 1, "should only insert 1 row")
      val nc = fixture.table.count
      //println(s"$tname result is new=$nc inserted=$i old count=$c ")
      assert(nc >= c + i, s"Bad result at $nc $i $c $tname")
      /*
      Note that the old count of rows plus the inserted need not equal the new count
      The old count is out of date at the time the new count is being read.
       */
      nc >= c + i
    }
  }

  it should "support consistent parallel threaded access" in {
    val effects = for {
      i <- (0 until 100)
    } yield(parallelEffect(i))

    effects.foreach { effect =>
      assert(Await.result(effect, 10 seconds) === true)
    }
    assert(fixture.table.count === 100, "Should only be 100 rows inserted")
  }



}