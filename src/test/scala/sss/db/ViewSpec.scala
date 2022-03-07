package sss.db

import org.scalatest.DoNotDiscover

import java.util.Date
import scala.concurrent.ExecutionException

@DoNotDiscover
class ViewSpec extends DbSpecQuickSetup {

  override def beforeEach(): Unit = {
    super.beforeEach()
    (for {
      _ <- db.createView("CREATE VIEW testview2 AS SELECT id, strId, intVal FROM test WHERE intVal > 50")
      _ <- persistIntVals(0 to 100)
    } yield ()).runSyncAndGet
  }

  "A View " should "support counting and filtering" in {
    val view: View = db.view("testview2")
    assert(view.count.runSyncAndGet == 50)
    val empty = view.filter(where("intVal < 50")).runSyncAndGet
    assert(empty.isEmpty)

  }

  "A View with baseWere clause " should "support counting and filtering" in {
    val view = db.view("testview2", where("intVal < 81"))
    assert(view.count.runSyncAndGet == 30)
    val empty = view.filter(where("intVal < 50")).runSyncAndGet
    assert(empty.isEmpty)
  }

  it should "support getting max id" in {
    val view = db.view("testview2", where("intVal < 81"))
    assert(view.maxId().runSyncAndGet == 81)
  }

  "A View with baseWhere with parameters" should "support getting max id and counting" in {
    val view = db.view("testview2", where("intVal < ?", 81))
    assert(view.count.runSyncAndGet == 30)
    assert(view.maxId().runSyncAndGet == 81)
  }

  "A View of a real table with baseWhere clause" should "support counting" in {
    val view = db.view("test", where("intVal < 81"))
    assert(view.count.runSyncAndGet == 81)
    assert(view.filter(where("intVal < 50")).runSyncAndGet.size == 50)
  }

  it should "support getting max id" in {
    val view = db.view("test", where("intVal < 81"))
    assert(view.maxId().runSyncAndGet == 81)
  }

  "An UpdatableView " should "support updating" in {
    val view = db.updatableView("test", where("intVal < 81"))
    assert(
      view
        .update(Map("strId" -> "strId", "createTime" -> new Date(), "intVal" -> -30), where("intVal=50"))
        .runSyncAndGet == 1
    )
    assert(view.updateRow(Map("id" -> 52, "strId" -> "strId", "createTime" -> new Date(), "intVal" -> -31)).runSyncAndGet.int("intVal") == -31)
    assert(view(51).runSyncAndGet.int("intVal") == -30)
    assert(view(52).runSyncAndGet.int("intVal") == -31)
  }

  it should "disallow updating rows outside the view" in {
    val view = db.updatableView("test", where("intVal < 81"))
    assert(
      view
        .update(Map("strId" -> "strId", "createTime" -> new Date(), "intVal" -> -30), where("intVal=83"))
        .runSyncAndGet == 0
    )
    assert(
      intercept[DbException](
        view.updateRow(Map("id" -> 82, "strId" -> "strId", "createTime" -> new Date(), "intVal" -> -30)).runSyncAndGet
      ).getMessage.contains("82")
    )
    assert(view.getRow(84).runSyncAndGet.isEmpty)
  }

  "An InsertableView" should "support inserting new rows" in {
    val view = db.insertableView("test", where("intVal < 102"))
    assert(view.count.runSyncAndGet == 101)
    view.insert(Map("strId" -> "strId", "intVal" -> -30)).runSyncAndGet
    assert(view.count.runSyncAndGet == 102)
    assert(view.insert(108, "strId", new Date(), 107).runSyncAndGet == 1)
    assert(view.count.runSyncAndGet == 102)
    val fullView = db.insertableView("test")
    assert(fullView.count.runSyncAndGet == 103)
  }

  it should "disallow inserting rows which will end up outside the view" in {
    val view = db.insertableView("test", where("intVal < 102"))
    val fullView = db.insertableView("test")
    intercept[ExecutionException](view.insert(Map("strId" -> "strId", "intVal" -> 103)).runSyncAndGet)
    assert(view.count.runSyncAndGet == 101)
    assert(fullView.count.runSyncAndGet == 101)
  }
}
