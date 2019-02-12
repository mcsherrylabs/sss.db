package sss.db


import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

object ExecutionContextHelper {

  implicit val synchronousExecutionContext = ExecutionContext.fromExecutor(task => task.run())

  implicit val ioExecutionContext = ExecutionContext
    .fromExecutorService(
      Executors.newCachedThreadPool()
    )

}
