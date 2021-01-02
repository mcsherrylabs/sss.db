package sss.db

import javax.sql.DataSource
import sss.db.PagedView.ToIterator

import scala.util.{Failure, Success}

/**
  * Created by alan on 6/21/16.
  */
trait Page {
  val rows: Rows
  val next: FutureTx[Option[Page]]
  val prev: FutureTx[Option[Page]]
  val hasNext: FutureTx[Boolean] = next.map(_.exists(_ => true))
  val hasPrev: FutureTx[Boolean] = prev.map(_.exists(_ => true))
  /**
    * An enclosing tx may be necessary if the rows contain Blobs, and early blob freeing is not
    * true. In this case, the blobs are only guaranteed to live until the tx is closed.
    */

}

private case class PageImpl private(indexCol: String,
                                    view: Query,
                                    rows: Rows,
                                    pageSize: Int,
                                    filter: Where) extends Page {

  require(rows.nonEmpty, "The EmptyPage handles no row situations.")

  lazy private val firstIndexInPage = rows.head[Number](indexCol).longValue
  lazy private val lastIndexInPage = rows.last[Number](indexCol).longValue

  lazy override val next: FutureTx[Option[Page]] = {
    nextRows.map(rs =>
      if (rs.isEmpty) None
      else Some(PageImpl(indexCol, view, rs, pageSize, filter))
    )
  }

  lazy override val prev: FutureTx[Option[Page]] = {

    prevRows.map(rs =>
      if (rs.isEmpty) None
      else Some(PageImpl(indexCol, view, rs.reverse, pageSize, filter)))
  }


  private lazy val nextRows = view.filter(
    where(s"$indexCol > ?", lastIndexInPage) and filter
      orderBy OrderAsc(indexCol)
      limit pageSize)


  private lazy val prevRows = view.filter(
    where(s"$indexCol < ?", firstIndexInPage) and filter
      orderBy OrderDesc(indexCol)
      limit pageSize)


}

object PagedView {

  def apply(view: Query, pageSize: Int, filter: Where = where(), indexCol: String = "id"): PagedView = {
    new PagedView(view, pageSize, filter, indexCol)
  }


  /**
    *
    * @param pagedView
    * @return
    */
  implicit class ToIterator(val pagedView: PagedView) extends AnyVal {
    def toIterator(implicit dataSource: DataSource): Iterator[Row] = ToStream(pagedView).toStream.iterator
  }

  implicit class ToStream(val pagedView: PagedView) extends AnyVal {

    def toStream(implicit dataSource: DataSource): LazyList[Row] = {

      def toStream(fPage: FutureTx[Option[Page]]): LazyList[Rows] = {

        fPage.runSync match {
          case Failure(e) => LazyList.empty[Rows]
          case Success(pageOpt) => pageOpt match {
            case Some(page) => page.rows #:: toStream(page.next)
            case None => LazyList.empty[Rows]
          }
        }
      }
      toStream(pagedView.firstPage).flatten
    }
  }

}

class PagedView private(view: Query,
                        pageSize: Int,
                        filter: Where,
                        indexCol: String) {

  /**
    * An enclosing tx may be necessary if the rows contain Blobs, and early blob freeing is not
    * true. In this case, the blobs are only guaranteed to live until the tx is closed.
    */


  def iterator(implicit dataSource: DataSource): Iterator[Row] = new ToIterator(this).toIterator

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
