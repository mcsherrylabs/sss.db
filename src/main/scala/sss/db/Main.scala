package sss.db

object Main {

  def main(args: Array[String]): Unit = {
    val plan = for {
      i <- 0 to 10
      if i > 100
      j <- i to 100
    } yield j

    println(plan)
  }
}
