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
    view.delete(where("1 = 1"))
    val pv = new PagedView(idCol, view, 2, (s"$statusCol = ? ", Seq(0)))
    var page = pv.last
    assert(!page.hasNext)
    assert(!page.hasPrev)
    assert(page.rows.isEmpty)
    page = pv.first
    assert(!page.hasNext)
    assert(!page.hasPrev)
    assert(page.rows.isEmpty)
  }

  it should " be able to scroll back through 5 pages " in {


    (1 to 10) foreach { i => view.insert(Map(idCol -> i, statusCol -> 0)) }

    val pv = new PagedView(idCol, view, 2, (s"$statusCol = ?" -> Seq(0)))
    var page = pv.last
    assert(!page.hasNext)
    assert(page.hasPrev)
    assert(page.rows.size == 2)
    var p = page
    val ps = (1 until 5) map { i =>
      p = p.prev
      assert(p.rows.size == 2)
      p
    }
    assert(!ps.last.hasPrev)
    assert(ps.last.hasNext)
  }


  it should " match expected walk exactly " in {


    (1 to 10) foreach { i => view.insert(Map(idCol -> i, statusCol -> 0)) }
    val pv = new PagedView(idCol, view, 3, (s"$statusCol = ?", Seq(0)))
    var page = pv.last
    assert(page.rows.size == 3)
    assert(page.rows(0)[Int](idCol) == 8)
    assert(page.rows(1)[Int](idCol) == 9)
    assert(page.rows(2)[Int](idCol) == 10)

    page = page.prev
    assert(page.rows.size == 3)
    assert(page.rows(0)[Int](idCol) == 5)
    assert(page.rows(1)[Int](idCol) == 6)
    assert(page.rows(2)[Int](idCol) == 7)

    page = page.prev
    assert(page.rows.size == 3)
    assert(page.rows(0)[Int](idCol) == 2)
    assert(page.rows(1)[Int](idCol) == 3)
    assert(page.rows(2)[Int](idCol) == 4)

    page = page.prev
    assert(page.rows.size == 1)
    assert(page.rows(0)[Int](idCol) == 1)

    page = page.next
    assert(page.rows.size == 3)
    assert(page.rows(0)[Int](idCol) == 2)
    assert(page.rows(1)[Int](idCol) == 3)
    assert(page.rows(2)[Int](idCol) == 4)


  }

  it should " respect the status column " in {

    (1 to 10) foreach { i => view2.insert(Map(idCol -> i, statusCol -> 0)) }
    Seq(2,4,6,8) foreach { i => view2.update(Map(idCol -> i, statusCol -> 1)) }

    val pv = new PagedView(idCol, view2, 2, (s"$statusCol = ?", Seq(0)))
    var page = pv.last
    assert(!page.hasNext)
    assert(page.hasPrev)
    assert(page.rows.size == 2)
    var p = page
    val ps = (1 until 3) map { i =>
      p = p.prev
      assert(p.rows.size == 2)
      println(p.rows.mkString(","))
      p
    }
    assert(!ps.last.hasPrev)
    assert(ps.last.hasNext)
  }


  it should " work without a filter " in {

    (1 to 10) foreach { i => view2.insert(Map(idCol -> i, statusCol -> 0)) }

    val pv = PagedView(view2, 2)
    var page = pv.first
    assert(page.hasNext)
    assert(!page.hasPrev)
    assert(page.rows.size == 2)
    var p = page
    (1 until 5) foreach{ n =>
      p = p.next
      assert(p.rows.size == 2)
      println(p.rows.mkString(","))
    }

  }

}
