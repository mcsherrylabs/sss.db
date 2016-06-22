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

case class PageImpl(indexCol: String, view: View, rows: Rows, pageSize: Int, filter: (String, Seq[Any])) extends Page {

  require(!rows.isEmpty, "The EmptyPage handles no row situations.")

  private val firstIndexInPage = rows.head[Number](indexCol).longValue
  private val lastIndexInPage = rows.last[Number](indexCol).longValue

  lazy private val filterClause = if(filter._1.isEmpty) "" else s" AND ${filter._1}"

  lazy override val next: Page =
    if(hasNext) PageImpl(indexCol, view, nextRows, pageSize, filter)
    else throw new IllegalAccessException("No next page")

  private lazy val nextRows = view.filter(
    where (s"$indexCol > ? ${filterClause} ORDER BY $indexCol ASC LIMIT $pageSize")
      using ((lastIndexInPage +: filter._2):_*))

  private lazy val prevRows = view.filter(
    where (s"$indexCol < ? ${filterClause} ORDER BY $indexCol DESC LIMIT $pageSize")
      using ((firstIndexInPage +: filter._2):_*))


  lazy override val prev: Page = {
    if(hasPrev) PageImpl(indexCol, view, prevRows.reverse, pageSize, filter)
    else throw new IllegalAccessException("No previous page")
  }

  lazy override val hasPrev: Boolean = !prevRows.isEmpty

  lazy override val hasNext: Boolean = !nextRows.isEmpty
}

case class EmptyPage(pagedView: PagedView) extends Page {
  override val rows: Rows = IndexedSeq()
  lazy override val next: Page = pagedView.last
  lazy override val prev: Page = pagedView.first
  override val hasPrev: Boolean = false
  override val hasNext: Boolean = false
}

object PagedView {
  def apply(view:View, pageSize: Int, filter: (String, Seq[Any]) = ("", Seq()), indexCol: String = "id") = {
    new PagedView(indexCol, view, pageSize, filter)
  }
}

class PagedView(indexCol: String, view:View,
               pageSize: Int, filter: (String, Seq[Any])) {

  def last: Page = {

    val rows = view.filter(where (s"${filter._1} ORDER BY $indexCol DESC LIMIT $pageSize") using (filter._2: _*))
    if(rows.isEmpty) EmptyPage(this)
    else PageImpl(indexCol, view, rows.reverse, pageSize, filter)
  }

  def first: Page = {
    val rows = view.filter(where (s"${filter._1} ORDER BY $indexCol ASC LIMIT $pageSize") using (filter._2: _*))
    if(rows.isEmpty) EmptyPage(this)
    else PageImpl(indexCol, view, rows, pageSize, filter)
  }
}
