package scalastyanax

import com.netflix.astyanax.{connectionpool => astxcp}

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 31.08.2013
 * Time: 13:03
 * To change this template use File | Settings | File Templates.
 */

case class OperationResult[R](astx: astxcp.OperationResult[R]) {

  def latency = astx.getLatency

  def attemptsCount = astx.getAttemptsCount

}

trait Execution[R]

case class Success[R](data: R, operationResult: OperationResult[R]) extends Execution[R]

case class Failure[R](throwable: Throwable) extends Execution[R]

object Execution {

  def wrap[R](result: => astxcp.OperationResult[R]): Execution[R] = {
    try {
      Success(result.getResult, OperationResult(result))
    } catch {
      case throwable: Throwable => Failure(throwable)
    }
  }
}
