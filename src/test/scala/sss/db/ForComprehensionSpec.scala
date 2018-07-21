package sss.db

import org.scalatest.DoNotDiscover

@DoNotDiscover
class ForComprehensionSpec extends DbSpecSetup {


  "A paged view " should "support the for comprehension style" in {

    val rows = for {
      x <- (0 until 10)
      _ = fixture.dbUnderTest.table("testForComp").insert(x, x+1, "strId" + x)
      //rs: Rows <- (fixture.dbUnderTest.table("testForComp").toPaged(15))
      rs: Rows <- (fixture.dbUnderTest.table("testForComp").toPaged(2)).toStream
      r        <- rs
    } yield(r)

    assert(rows.size === 100, "Should be 100 rows!")

  }

}