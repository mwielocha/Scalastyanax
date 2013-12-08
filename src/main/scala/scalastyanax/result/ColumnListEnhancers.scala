package scalastyanax.result

import com.netflix.astyanax.model.{Column, ColumnList}
import scala.collection.JavaConversions._
import reflect.runtime.universe._
import java.util.Date


/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 30.11.2013
 * Time: 21:53
 * To change this template use File | Settings | File Templates.
 */

object ColumnListImplicits extends ColumnListEnhancers

trait ColumnListEnhancers extends ColumnEnhancers {

  implicit class EnhancedColumnList[C](val columnList: ColumnList[C]) {

    def apply[T : TypeTag](column: C): Option[T] = {
      typeOf[T] match {
        case t if t =:= typeOf[String] => Option(columnList.getStringValue(column, null).asInstanceOf[T])
        case t if t =:= typeOf[Long] => Option(columnList.getLongValue(column, null).asInstanceOf[T])
        case t if t =:= typeOf[Int] => Option(columnList.getIntegerValue(column, null).asInstanceOf[T])
        case t if t =:= typeOf[Date] => Option(columnList.getDateValue(column, null).asInstanceOf[T])
        case t if t =:= typeOf[Double] => Option(columnList.getDoubleValue(column, null).asInstanceOf[T])
        case t if t =:= typeOf[Boolean] => Option(columnList.getBooleanValue(column, null).asInstanceOf[T])
        case otherwise => throw new IllegalArgumentException(s"Usupported column type: ${otherwise}")
      }
    }

    def apply(index: Int): Option[Column[C]] = Option(columnList.getColumnByIndex(index))

    def columnNames: Iterable[C] = columnList.getColumnNames

    def values[R : TypeTag]: Iterable[R] = columnList.flatMap(_.value[R])

    def mapColumnNames[R](mapper: C => R): Iterable[R] = {
      columnList.getColumnNames.map(mapper)
    }

    def mapValues[V : TypeTag, R](mapper: V => R): Iterable[Option[R]] = {
      columnList.map(_.value[V].map(mapper))
    }

    def flatMapValues[V : TypeTag, R](mapper: V => R): Iterable[R] = {
      mapValues(mapper).flatten
    }
  }
}
