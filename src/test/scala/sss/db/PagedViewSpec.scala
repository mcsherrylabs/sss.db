package sss.db

import org.scalatest.DoNotDiscover


/**
  * Created by alan on 6/21/16.
  */
@DoNotDiscover
class PagedViewSpec extends DbSpecSetup {

  def view = fixture.dbUnderTest.table(testPaged)

  def view2 = fixture.dbUnderTest.table(testPaged2)


  "A paged View" should " be able to handle an empty table " in {

    val db = fixture.dbUnderTest
    import db.runContext._

    view.delete(where("1 = 1"))
    val pv = PagedView(view, 2, where(s"$statusCol = ? ", 0), idCol)
    var page = pv.lastPage.runSyncAndGet
    assert(page.isEmpty)

    page = pv.firstPage.runSyncAndGet
    assert(page.isEmpty)

  }

  it should " be able to scroll back through 5 pages " in {

    val db = fixture.dbUnderTest
    import db.runContext._

    (1 to 10) foreach { i => view.insert(Map(idCol -> i, statusCol -> 0)).runSyncAndGet }

    val pv = PagedView(view, 2, where(statusCol -> 0), idCol)
    var page = pv.lastPage.runSyncAndGet.get
    assert(!page.hasNext.runSyncAndGet)
    assert(page.hasPrev.runSyncAndGet)
    assert(page.rows.size == 2)

    val ps = (1 until 5) map { i =>
      page = page.prev.runSyncAndGet.get
      assert(page.rows.size == 2)
      page
    }
    assert(!ps.last.hasPrev.runSyncAndGet)
    assert(ps.last.hasNext.runSyncAndGet)
  }


  it should " match expected walk exactly " in {

    val db = fixture.dbUnderTest
    import db.runContext._

    (1 to 10) foreach { i => view.insert(Map(idCol -> i, statusCol -> 0)).runSyncAndGet  }
    val pv = PagedView(view, 3, where (statusCol -> 0), idCol)
    var page = pv.lastPage.runSyncAndGet.get
    assert(page.rows.size == 3)
    assert(page.rows(0)[Int](idCol) == 8)
    assert(page.rows(1)[Int](idCol) == 9)
    assert(page.rows(2)[Int](idCol) == 10)

    page = page.prev.runSyncAndGet.get
    assert(page.rows.size == 3)
    assert(page.rows(0)[Int](idCol) == 5)
    assert(page.rows(1)[Int](idCol) == 6)
    assert(page.rows(2)[Int](idCol) == 7)

    page = page.prev.runSyncAndGet.get
    assert(page.rows.size == 3)
    assert(page.rows(0)[Int](idCol) == 2)
    assert(page.rows(1)[Int](idCol) == 3)
    assert(page.rows(2)[Int](idCol) == 4)

    page = page.prev.runSyncAndGet.get
    assert(page.rows.size == 1)
    assert(page.rows(0)[Int](idCol) == 1)

    page = page.next.runSyncAndGet.get
    assert(page.rows.size == 3)
    assert(page.rows(0)[Int](idCol) == 2)
    assert(page.rows(1)[Int](idCol) == 3)
    assert(page.rows(2)[Int](idCol) == 4)


  }

  it should " respect the status column " in {

    val db = fixture.dbUnderTest
    import db.runContext._

    (1 to 10) foreach { i => view2.insert(Map(idCol -> i, statusCol -> 0)).runSyncAndGet  }
    Seq(2,4,6,8) foreach { i => view2.updateRow(Map(idCol -> i, statusCol -> 1)).runSyncAndGet  }

    val pv = PagedView(view2, 2, where (statusCol -> 0), idCol)
    var page = pv.lastPage.runSyncAndGet.get
    assert(!page.hasNext.runSyncAndGet)
    assert(page.hasPrev.runSyncAndGet)
    assert(page.rows.size == 2)

    val ps = (1 until 3) map { i =>
      page = page.prev.runSyncAndGet.get
      assert(page.rows.size == 2)
      //println(page.rows.mkString(","))
      page
    }
    assert(!ps.last.hasPrev.runSyncAndGet)
    assert(ps.last.hasNext.runSyncAndGet)
  }


  it should " work without a filter " in {

    val db = fixture.dbUnderTest
    import db.runContext._

    (1 to 10) foreach { i => view2.insert(Map(idCol -> i, statusCol -> 0)).runSyncAndGet }

    val pv = PagedView(view2, 2)
    val page = pv.firstPage.runSyncAndGet.get
    assert(page.hasNext.runSyncAndGet)
    assert(!page.hasPrev.runSyncAndGet)
    assert(page.rows.size == 2)
    var p = page
    (1 until 5) foreach{ n =>
      p = p.next.runSyncAndGet.get
      assert(p.rows.size == 2)
      //println(p.rows.mkString(","))
    }

  }
}

