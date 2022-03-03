package sss.db

import org.scalatest.DoNotDiscover

@DoNotDiscover
class QuerySpec extends DbSpecQuickSetup {

  "Query with nonempty baseWhere clause " should "support filtering" in {
    val query = db.select("SELECT * FROM test", where("intVal > 50"))
    persistIntVals(0 to 100).runSync
    assert(query.filter().runSyncAndGet.size == 50)
    assert(query.filter(where("intVal < 50")).runSyncAndGet.isEmpty)
    assert(query.filter(where("intVal < 76")).runSyncAndGet.size == 25)
  }
}
