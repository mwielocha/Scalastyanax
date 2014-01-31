package scalastyanax.result

import com.netflix.astyanax.model.Column
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

    def value[T : Manifest]: Option[T] = {
      manifest match {
        case m if m <:< manifest[Long] => Option(column.getLongValue.asInstanceOf[T])
        case m if m <:< manifest[String] => Option(column.getStringValue.asInstanceOf[T])
        case m if m <:< manifest[Int] => Option(column.getIntegerValue.asInstanceOf[T])
        case m if m <:< manifest[Date] => Option(column.getDateValue.asInstanceOf[T])
        case m if m <:< manifest[Double] => Option(column.getDoubleValue.asInstanceOf[T])
        case m if m <:< manifest[Boolean] => Option(column.getBooleanValue.asInstanceOf[T])
        case otherwise => throw new IllegalStateException(s"Invalid column type: ${otherwise}")
      }
    }

    def map[V : Manifest, R](mapper: V => R): Option[R] = {
      value[V].map(mapper)
    }
  }
}
