package sss.db

trait ForComprehensionSpec {

  self: DbSpec =>

  it should "support the for comprehension style" in {

    (0 until 10) map { x =>
      fixture.dbUnderTest.table("testForComp").insert(x, x+1, "strId" + x)
    }

    val rows = for {
      rs: Rows <- (fixture.dbUnderTest.table("testForComp").toPaged(1))
      r        <- rs

      //r1 <- fixture.dbUnderTest.table("testForComp")
      //if r1[Long](idCol) == r2[Long](idCol)
      //if (r[Long](idCol) == 0)

    } yield((r, rs))


    assert(rows.size === 10, "Should only be one row!")

  }

}