package sss.db

import sss.db.PagedView.ToIterator

import scala.concurrent.Future

/**
  * Created by alan on 6/21/16.
  */
trait Page {
  val rows: Rows
  val next: FutureTx[Option[Page]]
  val prev: FutureTx[Option[Page]]
  val hasNext: FutureTx[Boolean] = next.map (_.exists(_ => true))
  val hasPrev: FutureTx[Boolean] = prev.map (_.exists(_ => true))
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


  lazy override val next: FutureTx[Option[Page]] = {
    nextRows.map (rows =>
      if (rows.isEmpty) None
      else Some(PageImpl(indexCol, view, rows, pageSize, filter))
    )
  }

  lazy override val prev: FutureTx[Option[Page]] = {

    prevRows.map(rs =>
      if(rs.isEmpty) None
      else Some(PageImpl(indexCol, view, rs.reverse, pageSize, filter)))
  }


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
  lazy override val next: FutureTx[Option[Page]] = pagedView.lastPage
  lazy override val prev: FutureTx[Option[Page]] = pagedView.firstPage
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
    def toIterator: FutureTx[Iterator[Rows]] = ToStream(pagedView).toFutureTxStream.map(_.iterator)
  }

  implicit class ToStream(val pagedView: PagedView) extends AnyVal {

    //There is no Stream.unfold in std lib as of 2.13
    def toFutureTxStream: FutureTx[Stream[Rows]] = {

      def toStream(fPage: => FutureTx[Option[Page]]): FutureTx[Stream[Rows]] = {
        fPage.flatMap {
          case None => FutureTx.unit(Stream.empty[Rows])
          case Some(page) =>
            if (page.rows.isEmpty) FutureTx.unit(Stream.empty[Rows])
            else { context =>
              import context.ec
              page.next.map(println(_))(context)
              toStream(page.next)(context) map { tail =>
                Stream.cons(page.rows, tail)
              }
            }
        }

      }

      toStream(pagedView.firstPage)
    }
  }

}

class PagedView private ( view:Query,
                          pageSize: Int,
                          filter: Where,
                          indexCol: String) extends FutureTx[Iterable[Rows]] {

  override def apply(context: TransactionContext): Future[Iterable[Rows]] =
    futureTxIterator.map(_.toIterable)(context)

  /**
    * An enclosing tx may be necessary if the rows contain Blobs, and early blob freeing is not
    * true. In this case, the blobs are only guaranteed to live until the tx is closed.
    */


  def futureTxIterator: FutureTx[Iterator[Rows]] = new ToIterator(this).toIterator

  def lastPage: FutureTx[Option[Page]] = {

    view.filter(
      filter
        orderBy OrderDesc(indexCol)
        limit pageSize).map { rows: Rows =>
      if (rows.isEmpty) None
      else Some(PageImpl(indexCol, view, rows.reverse, pageSize, filter))
    }

  }

  def firstPage: FutureTx[Option[Page]] = {
    view.filter(
      filter
        orderBy OrderAsc(indexCol)
        limit pageSize).map { rows =>
      if (rows.isEmpty) None
      else Some(PageImpl(indexCol, view, rows, pageSize, filter))
    }
  }
}
