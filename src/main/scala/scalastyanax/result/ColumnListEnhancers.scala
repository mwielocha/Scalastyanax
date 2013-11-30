package scalastyanax.result

import com.netflix.astyanax.model.ColumnList
import reflect.runtime.universe._


/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 30.11.2013
 * Time: 21:53
 * To change this template use File | Settings | File Templates.
 */

object ColumnListImplicits extends ColumnListEnhancers

trait ColumnListEnhancers {

  implicit class EnhancedColumnList[C](val columnList: ColumnList[C]) {

    def apply[T : TypeTag](column: C, defaultValue: Option[T] = None): Option[T] = {
      typeOf[T] match {
        case t if t =:= typeOf[String] => wrapStringValue(column, defaultValue)
      }
    }

    private def wrapStringValue[T](column: C, defaultValue: Option[T]): Option[T] = {
      Option(columnList.getStringValue(column, defaultValue.getOrElse(null).asInstanceOf[String]).asInstanceOf[T])
    }
  }
}
