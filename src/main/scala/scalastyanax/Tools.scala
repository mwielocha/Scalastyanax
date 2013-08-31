package scalastyanax

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 31.08.2013
 * Time: 13:01
 * To change this template use File | Settings | File Templates.
 */
object Tools {

  def getOrElseNull[V](option: Option[V]): V = {
    option.getOrElse(null.asInstanceOf[V])
  }
}

object JavaInteropt {

  case class NullableOption[T](option: Option[T]) {

    def getOrElseNull = option.getOrElse(null.asInstanceOf[T])

  }

  implicit def option2NullableOption[T](option: Option[T]) = NullableOption(option)

}

