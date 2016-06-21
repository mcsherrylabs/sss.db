package sss.db

import scala.reflect.runtime.universe._
/**
  * Created by alan on 6/21/16.
  */

trait Page {
  val rows: Rows
  val next: Page
  val prev: Page
  val hasNext: Boolean
  val hasPrev: Boolean
}

case class PageImpl[T >: ColumnTypes : TypeTag](indexCol: String, view: View, rows: Rows, pageSize: Int, filter: (String, Any)) extends Page {

  require(!rows.isEmpty, "The EmptyPage handles no row situations.")

  private val firstIndexInPage = rows.head[T](indexCol)
  private val lastIndexInPage = rows.last[T](indexCol)

  lazy override val next: Page =
    if(hasNext) PageImpl(indexCol, view, nextRows, pageSize, filter)
    else throw new IllegalArgumentException("No next page")

  private lazy val nextRows = view.filter(
    where (s"$indexCol > ? AND ${filter._1} = ? ORDER BY $indexCol ASC LIMIT $pageSize")
      using (lastIndexInPage, filter._2))

  private lazy val prevRows = view.filter(
    where (s"$indexCol < ? AND ${filter._1} = ? ORDER BY $indexCol DESC LIMIT $pageSize")
      using (firstIndexInPage, filter._2))


  lazy override val prev: Page = {
    if(hasPrev) PageImpl(indexCol, view, prevRows.reverse, pageSize, filter)
    else throw new IllegalArgumentException("No previous page")
  }

  lazy override val hasPrev: Boolean = !prevRows.isEmpty

  lazy override val hasNext: Boolean = !nextRows.isEmpty
}

case class EmptyPage(pagedView: PagedView[_ >: ColumnTypes]) extends Page {
  override val rows: Rows = IndexedSeq()
  lazy override val next: Page = pagedView.last
  lazy override val prev: Page = pagedView.first
  override val hasPrev: Boolean = false
  override val hasNext: Boolean = false
}

object PagedView {
  def apply(view:View, pageSize: Int, filter: (String, Any), indexCol: String = "id") = {
    new PagedView[Long](indexCol, view, pageSize, filter)
  }
}

class PagedView[T >: ColumnTypes : TypeTag](indexCol: String, view:View,
               pageSize: Int, filter: (String, Any)) {

  def last: Page = {
    val rows = view.filter(where (s"${filter._1} = ? ORDER BY $indexCol DESC LIMIT $pageSize") using (filter._2))
    if(rows.isEmpty) EmptyPage(this)
    else PageImpl[T](indexCol, view, rows.reverse, pageSize, filter)
  }

  def first: Page = {
    val rows = view.filter(where (s"${filter._1} = ? ORDER BY $indexCol ASC LIMIT $pageSize") using (filter._2))
    if(rows.isEmpty) EmptyPage(this)
    else PageImpl(indexCol, view, rows, pageSize, filter)
  }
}
