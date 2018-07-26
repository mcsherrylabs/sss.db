import sss.db._

object CheckVisibility {

  // Check the compiler visibility of the main classes from outside the sss.db package .
  val db = Db()
  val t : Table = db.table("")
  val v : View = db.view("")
  val q : Query = db.select("")
  val w: Where = where("", OrderDesc(""), OrderAsc(""))

}
