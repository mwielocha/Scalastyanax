package scalastyanax.operations

import com.netflix.astyanax.model.{Row, Rows, ColumnFamily}
import com.netflix.astyanax.{MutationBatch, Keyspace}
import com.netflix.astyanax.query.{AllRowsQuery, RowSliceQuery, RowQuery, ColumnQuery}
import scala.collection.JavaConversions._
import com.netflix.astyanax.recipes.reader.AllRowsReader
import java.lang

/**
 * author mikwie
 *
 */
object ColumnFamilyImplicits extends ColumnFamilyEnhancers

trait ColumnFamilyEnhancers {

  implicit class MutableColumnFamily[K, C](val columnFamily: ColumnFamily[K, C]) {

    def +=(rowKey: K, column: C, value: String, ttl: Option[Int] = None)(implicit keyspace: Keyspace) = {
      keyspace.prepareColumnMutation(columnFamily, rowKey, column)
        .putValue(value, ttl.getOrElse(null.asInstanceOf[Int]))
    }

    def +=(path: ((K, C), String))(implicit keyspace: Keyspace) = {
      path match {
        case ((rowKey, column), value) => {
          keyspace.prepareColumnMutation(columnFamily, rowKey, column)
            .putValue(value, null)
        }
      }
    }

    def ++=(path: ((K, C), String))(implicit mutationBatch: MutationBatch) = {
      path match {
        case ((rowKey, column), value) => {
          mutationBatch.withRow(columnFamily, rowKey)
            .putColumn(column, value, null)
        }
      }
    }

    def ++=(rowKey: K, column: C, value: String, ttl: Option[Int] = None)(implicit mutationBatch: MutationBatch) = {
      mutationBatch.withRow(columnFamily, rowKey)
        .putColumn(column, value, ttl.getOrElse(null.asInstanceOf[Int]))
    }
  }

  implicit class QueryableColumnFamily[K, C](val columnFamily: ColumnFamily[K, C]) {

    def apply(rowKey: K)(implicit keyspace: Keyspace): RowQuery[K, C] = {
      keyspace.prepareQuery(columnFamily)
        .getRow(rowKey)
    }

    def apply(rowKey: K, column: C)(implicit keyspace: Keyspace): ColumnQuery[C] = {
      keyspace.prepareQuery(columnFamily)
        .getKey(rowKey)
        .getColumn(column)
    }

    def apply(rowKeys: Iterable[K])(implicit keyspace: Keyspace): RowSliceQuery[K, C] = {
      keyspace.prepareQuery(columnFamily)
        .getKeySlice(rowKeys)
    }

    def apply(rowKeys: Iterable[K])(columns: Iterable[C])(implicit keyspace: Keyspace): RowSliceQuery[K, C] = {
      keyspace.prepareQuery(columnFamily)
        .getKeySlice(rowKeys)
        .withColumnSlice(columns)
    }

    def foreach(function: Row[K, C] => Boolean)(implicit keyspace: Keyspace): AllRowsReader.Builder[K, C] = {
      new AllRowsReader.Builder(keyspace, columnFamily)
        .forEachRow(forEachRowWrapper(function))
    }

    def foreachPage(function: Rows[K, C] => Boolean)(implicit keyspace: Keyspace): AllRowsReader.Builder[K, C] = {
      new AllRowsReader.Builder(keyspace, columnFamily)
        .forEachPage(forEachPageWrapper(function))
    }

    private def forEachRowWrapper[K, C](function: Row[K, C] => Boolean) = {
      new com.google.common.base.Function[Row[K, C], java.lang.Boolean]() {
        def apply(input: Row[K, C]): lang.Boolean = {
          function(input)
        }
      }
    }

    private def forEachPageWrapper[K, C](function: Rows[K, C] => Boolean) = {
      new com.google.common.base.Function[Rows[K, C], java.lang.Boolean]() {
        def apply(input: Rows[K, C]): lang.Boolean = {
          function(input)
        }
      }
    }
  }
}

