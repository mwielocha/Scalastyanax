package scalastyanax.result

import com.netflix.astyanax.model.Column
import reflect.runtime.universe._
import java.util.Date

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 01.12.2013
 * Time: 13:04
 * To change this template use File | Settings | File Templates.
 */
object ColumnImplicits extends ColumnEnhancers


trait ColumnEnhancers {

  implicit class EnhancedColumn[C](val column: Column[C]) {

    def name: C = column.getName

    def value[T : TypeTag]: Option[T] = {
      typeOf[T] match {
        case t if t =:= typeOf[String] => Option(column.getStringValue.asInstanceOf[T])
        case t if t =:= typeOf[Long] => Option(column.getLongValue.asInstanceOf[T])
        case t if t =:= typeOf[Int] => Option(column.getIntegerValue.asInstanceOf[T])
        case t if t =:= typeOf[Date] => Option(column.getDateValue.asInstanceOf[T])
        case t if t =:= typeOf[Double] => Option(column.getDoubleValue.asInstanceOf[T])
        case t if t =:= typeOf[Boolean] => Option(column.getBooleanValue.asInstanceOf[T])
        case otherwise => throw new IllegalStateException(s"Invalid column type: ${otherwise}")
      }
    }

    def map[V : TypeTag, R](mapper: V => R): Option[R] = {
      value[V].map(mapper)
    }
  }
}
