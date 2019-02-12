package sss.db

import sss.db.PagedView.ToIterator

/**
  * Created by alan on 6/21/16.
  */
trait Page {
  val rows: Rows
  val next: Page
  val prev: Page
  val hasNext: Boolean
  val hasPrev: Boolean
  /**
    * An enclosing tx may be necessary if the rows contain Blobs, and early blob freeing is not
    * true. In this case, the blobs are only guaranteed to live until the tx is closed.
    */

}

private case class PageImpl private (indexCol: String,
                                     view: Query,
                                     rows: Rows,
                                     pageSize: Int,
                                     filter: Where) extends Page {

  require(rows.nonEmpty, "The EmptyPage handles no row situations.")

  import view.ds

  private val firstIndexInPage = rows.head[Number](indexCol).longValue
  private val lastIndexInPage = rows.last[Number](indexCol).longValue


  lazy override val next: Page =
    if(hasNext) PageImpl(indexCol, view, nextRows, pageSize, filter)
    else throw new IllegalAccessException("No next page")

  lazy override val prev: Page = {
    if(hasPrev) PageImpl(indexCol, view, prevRows.reverse, pageSize, filter)
    else throw new IllegalAccessException("No previous page")
  }

  lazy override val hasPrev: Boolean = prevRows.nonEmpty

  lazy override val hasNext: Boolean = nextRows.nonEmpty

  private lazy val nextRows = view.filter(
    where (s"$indexCol > ?", lastIndexInPage) and filter
      orderBy OrderAsc(indexCol)
      limit pageSize).run

  private lazy val prevRows = view.filter(
    where (s"$indexCol < ?", firstIndexInPage) and filter
      orderBy OrderDesc(indexCol)
      limit pageSize).run

}

case class EmptyPage(pagedView: PagedView) extends Page {
  override val rows: Rows = IndexedSeq()
  lazy override val next: Page = pagedView.lastPage
  lazy override val prev: Page = pagedView.firstPage
  override val hasPrev: Boolean = false
  override val hasNext: Boolean = false

}

object PagedView {

  def apply(view:Query, pageSize: Int, filter: Where = where(), indexCol: String = "id"): PagedView = {
    new PagedView(view, pageSize, filter, indexCol)
  }


  /**
    *
    * @param pagedView
    * @return
    */
  implicit class ToIterator(val pagedView: PagedView) extends AnyVal {
    def toIterator: Iterator[Rows] = ToStream(pagedView).toStream.iterator
  }

  implicit class ToStream(val pagedView: PagedView) extends AnyVal {

    //There is no Stream.unfold in std lib as of 2.13
    def toStream: Stream[Rows] = {
      def stream(p: Page): Stream[Rows] = {
        if (p.rows.isEmpty) Stream.empty
        else if (p.hasNext) p.rows #:: stream(p.next)
        else p.rows #:: Stream.empty[Rows]
      }

      stream(pagedView.firstPage)
    }
  }

}

class PagedView private ( view:Query,
                          pageSize: Int,
                          filter: Where,
                          indexCol: String) extends Iterable[Rows] {

  /**
    * An enclosing tx may be necessary if the rows contain Blobs, and early blob freeing is not
    * true. In this case, the blobs are only guaranteed to live until the tx is closed.
    */
  import view.ds

  override def iterator: Iterator[Rows] = new ToIterator(this).toIterator

  def lastPage: Page = {

    val rows = view.filter(
      filter
        orderBy OrderDesc(indexCol)
        limit pageSize).run

    if(rows.isEmpty) EmptyPage(this)
    else PageImpl(indexCol, view, rows.reverse, pageSize, filter)
  }

  def firstPage: Page = {
    val rows = view.filter(
      filter
        orderBy OrderAsc(indexCol)
        limit pageSize).run

    if(rows.isEmpty) EmptyPage(this)
    else PageImpl(indexCol, view, rows, pageSize, filter)
  }
}
