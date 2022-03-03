package sss.db

import org.scalatest.DoNotDiscover

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
    val view = db.viewWhere("testview2", where("intVal < 81"))
    assert(view.count.runSyncAndGet == 30)
    val empty = view.filter(where("intVal < 50")).runSyncAndGet
    assert(empty.isEmpty)
  }

  it should "support getting max id" in {
    val view = db.viewWhere("testview2", where("intVal < 81"))
    assert(view.maxId().runSyncAndGet == 81)
  }

  "A View of a real table with baseWhere clause" should "support counting" in {
    val view = db.viewWhere("test", where("intVal < 81"))
    assert(view.count.runSyncAndGet == 81)
    assert(view.filter(where("intVal < 50")).runSyncAndGet.size == 50)
  }

  it should "support getting max id" in {
    val view = db.viewWhere("test", where("intVal < 81"))
    assert(view.maxId().runSyncAndGet == 81)
  }
}
