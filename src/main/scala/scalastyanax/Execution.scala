package scalastyanax

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 31.08.2013
 * Time: 13:03
 * To change this template use File | Settings | File Templates.
 */
trait Execution[R]

case class Success[R](data: R) extends Execution[R]

case class Failure[R](throwable: Throwable) extends Execution[R]

object Execution {

  def wrap[R](execution: => R): Execution[R] = {
    try {
      Success(execution)
    } catch {
      case throwable: Throwable => Failure(throwable)
    }
  }
}
