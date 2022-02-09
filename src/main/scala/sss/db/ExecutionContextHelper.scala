package sss.db


import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext


object ExecutionContextHelper {

  implicit val ioExecutionContext = createThreadPoolExecutionContext()

  implicit val synchronousExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(task => task.run())

  def createThreadPoolExecutionContext(): ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
}
