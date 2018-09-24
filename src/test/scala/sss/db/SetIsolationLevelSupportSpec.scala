package sss.db

import java.sql.{Connection, SQLTransactionRollbackException}
import java.util.Date

import org.scalatest.DoNotDiscover

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

@DoNotDiscover
class SetIsolationLevelSupportSpec extends DbSpecSetup {

  def parallelEffect(j: Int): Try[(Long, Long)] = {
    Try {
      fixture.table.tx(Connection.TRANSACTION_SERIALIZABLE, {
        val tname = Thread.currentThread().getName
        val c = fixture.table.count
        val i = fixture.table.insert(j, s"strId $j", new Date().getTime, j)
        assert(i === 1, "should only insert 1 row")
        (c, fixture.table.count)
      })
      //println(s"$tname result is new=$nc inserted=$i old count=$c ")
    } recoverWith {
      case e: SQLTransactionRollbackException => {
        //println(e)
        parallelEffect(j)
      }
    }
  }


  def futureParallelEffect(j: Int):Future[Try[(Long, Long)]] = Future (parallelEffect(j))



  it should "support TRANSACTION_SERIALIZABLE " in {
    val testCount = 100
    val effects = for {
      i <- (0 until testCount)
    } yield(futureParallelEffect(i))

    effects.foreach { effect =>
      Await.result(effect, 10 seconds) match {
        case Failure(e) => fail("No idea? ")
        case Success(c) => assert(c._1 + 1 === c._2, "Counts should be incremental")
      }

    }
    assert(fixture.table.count === testCount, s"Should only be $testCount rows inserted")
  }



}