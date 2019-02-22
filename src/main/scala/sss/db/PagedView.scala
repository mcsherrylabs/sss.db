package sss.db

import sss.db.PagedView.ToIterator

/**
  * Created by alan on 6/21/16.
  */
trait Page {
  val rows: Rows
  val next: FutureTx[Page]
  val prev: FutureTx[Page]
  val hasNext: FutureTx[Boolean]
  val hasPrev: FutureTx[Boolean]
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

  private val firstIndexInPage = rows.head[Number](indexCol).longValue
  private val lastIndexInPage = rows.last[Number](indexCol).longValue


  lazy override val next: FutureTx[Page] = {

    hasNext.flatMap { b: Boolean =>
      if (b) nextRows.map(PageImpl(indexCol, view, _, pageSize, filter))
      else throw new IllegalAccessException("No next page")
    }
  }

  lazy override val prev: FutureTx[Page] = {
    hasPrev.flatMap { b: Boolean =>
      if (b) prevRows.map(rs => PageImpl(indexCol, view, rs.reverse, pageSize, filter))
      else throw new IllegalAccessException("No previous page")
    }
  }

  lazy override val hasPrev: FutureTx[Boolean] = prevRows.map(_.nonEmpty)

  lazy override val hasNext: FutureTx[Boolean] = nextRows.map(_.nonEmpty)

  private lazy val nextRows = view.filter(
      where (s"$indexCol > ?", lastIndexInPage) and filter
        orderBy OrderAsc(indexCol)
        limit pageSize)


  private lazy val prevRows = view.filter(
      where(s"$indexCol < ?", firstIndexInPage) and filter
        orderBy OrderDesc(indexCol)
        limit pageSize)


}

case class EmptyPage(pagedView: PagedView) extends Page {
  override val rows: Rows = IndexedSeq()
  lazy override val next: FutureTx[Page] = pagedView.lastPage
  lazy override val prev: FutureTx[Page] = pagedView.firstPage
  override val hasPrev: FutureTx[Boolean] = FutureTx.unit(false)
  override val hasNext: FutureTx[Boolean] = FutureTx.unit(false)

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
    def toIterator: Iterator[FutureTx[Rows]] = ToStream(pagedView).toStream.iterator
  }

  implicit class ToStream(val pagedView: PagedView) extends AnyVal {

    //There is no Stream.unfold in std lib as of 2.13
    def toStream: Stream[FutureTx[Rows]] = {

      def rowToStream(rows: Rows): Stream[Rows] = {
        if(rows.isEmpty) Stream.empty
        else Stream.cons(rows, Stream.empty)
      }

      def toStream(fPage: FutureTx[Page]): Stream[FutureTx[Rows]] = {
        Stream.cons(fPage.map(_.rows), toStream({
          fPage.flatMap { p =>
            p.hasNext.flatMap { next =>
              if (next) p.next
              else FutureTx.unit(EmptyPage(pagedView))
            }
          }
        }))
      }

      toStream(pagedView.firstPage)
    }
  }

}

class PagedView private ( view:Query,
                          pageSize: Int,
                          filter: Where,
                          indexCol: String) extends Iterable[FutureTx[Rows]] {

  /**
    * An enclosing tx may be necessary if the rows contain Blobs, and early blob freeing is not
    * true. In this case, the blobs are only guaranteed to live until the tx is closed.
    */
  import view.runContext.ds

  override def iterator: Iterator[FutureTx[Rows]] = new ToIterator(this).toIterator

  def lastPage: FutureTx[Page] = {

    view.filter(
      filter
        orderBy OrderDesc(indexCol)
        limit pageSize).map { rows: Rows =>
      if (rows.isEmpty) EmptyPage(this)
      else PageImpl(indexCol, view, rows.reverse, pageSize, filter)
    }

  }

  def firstPage: FutureTx[Page] = {
    view.filter(
      filter
        orderBy OrderAsc(indexCol)
        limit pageSize).map { rows =>
      if (rows.isEmpty) EmptyPage(this)
      else PageImpl(indexCol, view, rows, pageSize, filter)
    }
  }
}
