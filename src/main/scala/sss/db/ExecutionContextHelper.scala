package sss.db


import scala.concurrent.ExecutionContext

object ExecutionContextHelper {

  implicit val synchronousExecutionContext = ExecutionContext.fromExecutor(task => task.run())

}
