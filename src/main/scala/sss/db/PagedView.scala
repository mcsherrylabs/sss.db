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
  def tx[T](f: => T): T
}

private case class PageImpl private (indexCol: String,
                                     view: Query,
                                     rows: Rows,
                                     pageSize: Int,
                                     filter: Where) extends Page {

  require(!rows.isEmpty, "The EmptyPage handles no row situations.")

  private val firstIndexInPage = rows.head.number(indexCol).longValue
  private val lastIndexInPage = rows.last.number(indexCol).longValue

  override def tx[T](f: => T): T = view.tx[T](f)

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
      limit pageSize)

  private lazy val prevRows = view.filter(
    where (s"$indexCol < ?", firstIndexInPage) and filter
      orderBy OrderDesc(indexCol)
      limit pageSize)

}

case class EmptyPage(pagedView: PagedView) extends Page {
  override val rows: Rows = IndexedSeq()
  lazy override val next: Page = pagedView.lastPage
  lazy override val prev: Page = pagedView.firstPage
  override val hasPrev: Boolean = false
  override val hasNext: Boolean = false
  def tx[T](f: => T) = pagedView.tx(f)
}

object PagedView {

  def apply(view:Query, pageSize: Int, filter: Where = where(), indexCol: String = "id") = {
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
    def toStream: LazyList[Rows] = {
      def stream(p: Page): LazyList[Rows] = {
        if (p.rows.isEmpty) LazyList.empty
        else if (p.hasNext) p.rows #:: stream(p.next)
        else p.rows #:: LazyList.empty[Rows]
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
  def tx[T](f: => T): T = view.tx[T](f)

  override def iterator: Iterator[Rows] = new ToIterator(this).toIterator

  def lastPage: Page = {

    val rows = view.filter(
      filter
        orderBy OrderDesc(indexCol)
        limit pageSize)

    if(rows.isEmpty) EmptyPage(this)
    else PageImpl(indexCol, view, rows.reverse, pageSize, filter)
  }

  def firstPage: Page = {
    val rows = view.filter(
      filter
        orderBy OrderAsc(indexCol)
        limit pageSize)

    if(rows.isEmpty) EmptyPage(this)
    else PageImpl(indexCol, view, rows, pageSize, filter)
  }
}
