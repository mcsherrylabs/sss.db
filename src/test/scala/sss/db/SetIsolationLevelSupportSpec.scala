package sss.db

import java.sql.SQLTransactionRollbackException
import java.util.Date

import org.scalatest.DoNotDiscover

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

@DoNotDiscover
class SetIsolationLevelSupportSpec extends DbSpecSetup {

  def parallelEffect(j: Int): FutureTx[(Long,Long)] = {

      for {
        c <- fixture.table.count
        i <- fixture.table.insert(j, s"strId $j", new Date().getTime, j)
        _ = assert (i === 1, "should only insert 1 row")
        nc <- fixture.table.count
        //_ = println(s"${Thread.currentThread().getName} result is new=$nc inserted=$i old count=$c ")

      } yield (c, nc)

  }


  it should "support TRANSACTION_SERIALIZABLE " in {

    val db = fixture.dbUnderTest
    implicit val cntxt = db.runContext(TxIsolationLevel.SERIALIZABLE)
    import cntxt.ds
    import cntxt.executor


    val testCount = 100

    def runIt(effect: FutureTx[(Long, Long)]): Future[(Long, Long)] = {
      effect.run.recoverWith {
        case e: SQLTransactionRollbackException => {
          runIt(effect)
        }
      }
    }

    val effects = 0 until testCount map parallelEffect map runIt


    effects.foreach { effect =>
      Await.result(effect, 10 seconds) match {
        case (c1, c2) => assert(c1 + 1 === c2, "Counts should be incremental")
      }

    }

    assert(fixture.table.count.runSyncAndGet === testCount, s"Should only be $testCount rows inserted")
  }



}
