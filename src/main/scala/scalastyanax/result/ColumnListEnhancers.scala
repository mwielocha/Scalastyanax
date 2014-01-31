package scalastyanax.result

import com.netflix.astyanax.model.{Column, ColumnList}
import scala.collection.JavaConversions._
import java.util.Date


/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 30.11.2013
 * Time: 21:53
 * To change this template use Filme | Settings | File Templates.
 */

object ColumnListImplicits extends ColumnListEnhancers

trait ColumnListEnhancers extends ColumnEnhancers {

  implicit class EnhancedColumnList[C](val columnList: ColumnList[C]) {

    def apply[T : Manifest](column: C): Option[T] = {
      manifest match {
        case m if m <:< manifest[String] => Option(columnList.getStringValue(column, null).asInstanceOf[T])
        case m if m <:< manifest[Long] => Option(columnList.getLongValue(column, null).asInstanceOf[T])
        case m if m <:< manifest[Int] => Option(columnList.getIntegerValue(column, null).asInstanceOf[T])
        case m if m <:< manifest[Date] => Option(columnList.getDateValue(column, null).asInstanceOf[T])
        case m if m <:< manifest[Double] => Option(columnList.getDoubleValue(column, null).asInstanceOf[T])
        case m if m <:< manifest[Boolean] => Option(columnList.getBooleanValue(column, null).asInstanceOf[T])
        case otherwise => throw new IllegalArgumentException(s"Usupported column type: ${otherwise}")
      }
    }

    def apply(index: Int): Option[Column[C]] = Option(columnList.getColumnByIndex(index))

    def columnNames: Iterable[C] = columnList.getColumnNames

    def values[R : Manifest]: Iterable[R] = columnList.flatMap(_.value[R])

    def mapColumnNames[R](mapper: C => R): Iterable[R] = {
      columnList.getColumnNames.map(mapper)
    }

    def mapValues[V : Manifest, R](mapper: V => R): Iterable[Option[R]] = {
      columnList.map(_.value[V].map(mapper))
    }

    def flatMapValues[V : Manifest, R](mapper: V => R): Iterable[R] = {
      mapValues(mapper).flatten
    }
  }
}
