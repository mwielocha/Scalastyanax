package scalastyanax.common

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 26.11.2013
 * Time: 22:12
 * To change this template use File | Settings | File Templates.
 */
trait ExecutorHelper {

  def wrap[R](execution: Unit => R): Either[Throwable, R] = {
    try {
      Right(execution())
    } catch {
      case e: Exception => Left(e)
    }
  }
}
