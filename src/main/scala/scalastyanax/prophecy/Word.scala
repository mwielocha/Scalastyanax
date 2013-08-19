package prophecy

import com.netflix.astyanax.model.Column
import scala.reflect.ClassTag

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 19.08.2013
 * Time: 21:37
 * To change this template use File | Settings | File Templates.
 */
case class Word[C](val column: Column[C]) {

  def verbatim = column.getName

  def meaningOf[T](implicit manifest: Manifest[T]) = {
    manifest match {
      case m if(m == ClassTag(classOf[String])) => column.getStringValue
    }
  }

}
