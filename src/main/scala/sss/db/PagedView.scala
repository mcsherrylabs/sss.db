package sss.db

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
  def tx[T](f: => T)
}

case class PageImpl private (indexCol: String, view: Query, rows: Rows, pageSize: Int, filter: (String, Seq[Any])) extends Page {

  require(!rows.isEmpty, "The EmptyPage handles no row situations.")

  private val firstIndexInPage = rows.head[Number](indexCol).longValue
  private val lastIndexInPage = rows.last[Number](indexCol).longValue

  override def tx[T](f: => T) = view.tx[T](f)

  lazy private val filterClause = if(filter._1.isEmpty) "" else s" AND ${filter._1}"

  lazy override val next: Page =
    if(hasNext) PageImpl(indexCol, view, nextRows, pageSize, filter)
    else throw new IllegalAccessException("No next page")

  lazy override val prev: Page = {
    if(hasPrev) PageImpl(indexCol, view, prevRows.reverse, pageSize, filter)
    else throw new IllegalAccessException("No previous page")
  }

  lazy override val hasPrev: Boolean = !prevRows.isEmpty

  lazy override val hasNext: Boolean = !nextRows.isEmpty

  private lazy val nextRows = view.filter(
    where (s"$indexCol > ? ${filterClause}", (lastIndexInPage +: filter._2):_*)
      orderBy OrderAsc(indexCol)
      limit pageSize)

  private lazy val prevRows = view.filter(
    where (s"$indexCol < ? ${filterClause}", (firstIndexInPage +: filter._2):_*)
      orderBy OrderDesc(indexCol)
      limit pageSize)

}

case class EmptyPage(pagedView: PagedView) extends Page {
  override val rows: Rows = IndexedSeq()
  lazy override val next: Page = pagedView.last
  lazy override val prev: Page = pagedView.first
  override val hasPrev: Boolean = false
  override val hasNext: Boolean = false
  def tx[T](f: => T) = pagedView.tx(f)
}

object PagedView {
  def apply(view:Query, pageSize: Int, filter: (String, Seq[Any]) = ("", Seq()), indexCol: String = "id") = {
    new PagedView(indexCol, view, pageSize, filter)
  }

  /**
    * NOT thread safe.
    * 'hasNext' must be checked before calling 'next' to avoid IllegalAccessException
    *
    * @param pagedView
    * @return
    */
  implicit class ToIterator(val pagedView: PagedView) extends AnyVal {

    def toIterator: Iterator[Rows] = new Iterator[Rows] {
      var currentPage = pagedView.first

      override def next(): Rows = {
        val cached = currentPage.rows
        currentPage = currentPage.next
        cached
      }

      override def hasNext: Boolean = currentPage.hasNext
    }
  }



  implicit class ToStream(val pagedView: PagedView) extends AnyVal {

    //There is no Stream.unfold in std lib as of 2.13
    def toStream: Stream[Rows] = {
      def stream(p: Page): Stream[Rows] = {
        if (p.rows.isEmpty) Stream.empty
        else if (p.hasNext) p.rows #:: stream(p.next)
        else p.rows #:: Stream.empty[Rows]
      }

      stream(pagedView.first)
    }
  }

}

class PagedView(indexCol: String, view:Query,
               pageSize: Int, filter: (String, Seq[Any])) {

  /**
    * An enclosing tx may be necessary if the rows contain Blobs, and early blob freeing is not
    * true. In this case, the blobs are only guaranteed to live until the tx is closed.
    */
  def tx[T](f: => T) = view.tx[T](f)

  def last: Page = {

    val rows = view.filter(
      where (filter)
        orderBy OrderDesc(indexCol)
        limit pageSize)

    if(rows.isEmpty) EmptyPage(this)
    else PageImpl(indexCol, view, rows.reverse, pageSize, filter)
  }

  def first: Page = {
    val rows = view.filter(
      where (filter)
        orderBy OrderAsc(indexCol)
        limit pageSize)

    if(rows.isEmpty) EmptyPage(this)
    else PageImpl(indexCol, view, rows, pageSize, filter)
  }
}
