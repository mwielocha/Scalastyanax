package scalastyanax.result

import com.netflix.astyanax.model.{ColumnList, Row, Rows}
import scala.util.Try
import scala.collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 01.12.2013
 * Time: 13:52
 * To change this template use File | Settings | File Templates.
 */
object RowsImplicits extends RowsEnhancers

trait RowsEnhancers extends ColumnListEnhancers {

  implicit class EnhancedRows[K, C](val rows: Rows[K, C]) {

    def apply(index: Int): Option[Row[K, C]] = Try(rows.getRowByIndex(index)).toOption

    def apply(rowKey: K): Option[Row[K, C]] = Try(rows.getRow(rowKey)).toOption

    // TODO: what to do with IllegalStateException?
    def keys: Iterable[K] = rows.iterator.map(_.getKey).toIterable

    def map[R](mapper: Row[K, C] => R): Iterable[R] = rows.iterator().map(mapper).toIterable

    def flatValues[V : Manifest]: Iterable[V] = rows.flatMap(_.getColumns.values[V])

    def toMap: Map[K, ColumnList[C]] = rows.iterator.map(row => row.getKey -> row.getColumns).toMap

    def toValueMap[V : Manifest]: Map[K, Iterable[V]] = toMap.mapValues(_.values[V])

    def foreach(function: Row[K, C] => Unit) {
      rows.iterator().foreach(function(_))
    }
  }
}
