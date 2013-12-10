package scalastyanax.operations

import com.netflix.astyanax.model.{Row, Rows, ColumnFamily}
import com.netflix.astyanax.{Execution, ColumnListMutation, MutationBatch, Keyspace}
import com.netflix.astyanax.query.{RowSliceQuery, RowQuery, ColumnQuery}
import scala.collection.JavaConversions._
import com.netflix.astyanax.recipes.reader.AllRowsReader
import java.lang
import scalastyanax.RangeQuery
import reflect.runtime.universe._
import scala.annotation.implicitNotFound
import com.netflix.astyanax.connectionpool.OperationResult

/**
 * author mikwie
 *
 */
object ColumnFamilyImplicits extends ColumnFamilyEnhancers

trait ColumnFamilyEnhancers {

  implicit class MutableColumnFamily[K, C](val columnFamily: ColumnFamily[K, C]) {

    /**
     * Put value
     *
     * @param rowKey
     * @param column
     * @param value
     * @param ttl
     * @param keyspace
     * @param typeTagV
     * @tparam V
     * @return
     */

    def +=[V](rowKey: K, column: C, value: V, ttl: Option[Int] = None)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, typeTagV: TypeTag[V]): Execution[Void] = {
      val columnMutation =  keyspace.prepareColumnMutation(columnFamily, rowKey, column)
      typeOf[V] match {
        case t if t =:= typeOf[String] => columnMutation.putValue(value.asInstanceOf[String], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case t if t =:= typeOf[Long] => columnMutation.putValue(value.asInstanceOf[Long], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case t if t =:= typeOf[Int] => columnMutation.putValue(value.asInstanceOf[Int], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case t if t =:= typeOf[Double] => columnMutation.putValue(value.asInstanceOf[Double], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case t if t =:= typeOf[Boolean] => columnMutation.putValue(value.asInstanceOf[Boolean], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case t if t =:= typeOf[Float] => columnMutation.putValue(value.asInstanceOf[Float], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case otherwise => throw new IllegalArgumentException(s"Usupported value type: ${otherwise}")
      }
    }

    def +=[V](path: ((K, C), V))(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, typeTagV: TypeTag[V]): Execution[Void] = {
      path match {
        case ((rowKey, column), value) => {
          +=(rowKey, column, value)(keyspace, typeTagV)
        }
      }
    }

    /**
     * Put empty column
     *
     * @param rowKey
     * @param column
     * @param ttl
     * @param keyspace
     * @return
     */

    def +=(rowKey: K, column: C, ttl: Option[Int] = None)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): Execution[Void] = {
      keyspace.prepareColumnMutation(columnFamily, rowKey, column)
        .putEmptyColumn(ttl.map(int2Integer(_)).orNull[java.lang.Integer])
    }

    def +=(path: (K, C))(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): Execution[Void] = {
      path match {
        case (rowKey, column) => +=(rowKey, column, None)(keyspace)
      }
    }

      /**
     * Increment counter column
     *
     * @param rowKey
     * @param column
     * @param value
     * @param keyspace
     * @return
     */

    def ^=(rowKey: K, column: C, value: Long)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): Execution[Void] = {
      keyspace.prepareColumnMutation(columnFamily, rowKey, column).incrementCounterColumn(value)
    }

    def ^=(path: ((K, C), Long))(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): Execution[Void] = {
      path match {
        case ((rowKey, column), value) => ^=(rowKey, column, value)(keyspace)
      }
    }

    /**
     * Delete column
     *
     * @param rowKey
     * @param column
     * @param keyspace
     * @return
     */

    def -=(rowKey: K, column: C)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): Execution[Void] = {
      keyspace.prepareColumnMutation(columnFamily, rowKey, column)
        .deleteColumn()
    }

    def -=(path: (K, C))(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): Execution[Void] = {
      path match {
        case (rowKey, column) => -=(rowKey, column)(keyspace)
      }
    }

    /**
     * Batch put value
     *
     * @param rowKey
     * @param column
     * @param value
     * @param ttl
     * @param mutationBatch
     * @param typeTagV
     * @tparam V
     * @return
     */

    def ++=[V](rowKey: K, column: C, value: V, ttl: Option[Int] = None)(implicit @implicitNotFound("Mutation batch must be implicitly provided!") mutationBatch: MutationBatch, typeTagV: TypeTag[V]): ColumnListMutation[C] = {
      val columnListMutation = mutationBatch.withRow(columnFamily, rowKey)
      typeOf[V] match {
        case t if t =:= typeOf[String] => columnListMutation.putColumn(column, value.asInstanceOf[String], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case t if t =:= typeOf[Long] => columnListMutation.putColumn(column, value.asInstanceOf[Long], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case t if t =:= typeOf[Int] => columnListMutation.putColumn(column, value.asInstanceOf[Int], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case t if t =:= typeOf[Double] => columnListMutation.putColumn(column, value.asInstanceOf[Double], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case t if t =:= typeOf[Boolean] => columnListMutation.putColumn(column, value.asInstanceOf[Boolean], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case t if t =:= typeOf[Float] => columnListMutation.putColumn(column, value.asInstanceOf[Float], ttl.map(int2Integer(_)).orNull[java.lang.Integer])
        case otherwise => throw new IllegalArgumentException(s"Usupported value type: ${otherwise}")
      }
      columnListMutation
    }

    def ++=[V](path: ((K, C), V))(implicit @implicitNotFound("Mutation batch must be implicitly provided!") mutationBatch: MutationBatch, typeTagV: TypeTag[V]): ColumnListMutation[C] = {
      path match {
        case ((rowKey, column), value) => ++=(rowKey, column, value, None)(mutationBatch, typeTagV)
      }
    }

    /**
     * Batch put empty column
     *
     * @param rowKey
     * @param column
     * @param ttl
     * @param mutationBatch
     * @return
     */

    def ++=(rowKey: K, column: C, ttl: Option[Int] = None)(implicit @implicitNotFound("Mutation batch must be implicitly provided!") mutationBatch: MutationBatch): ColumnListMutation[C] = {
      mutationBatch.withRow(columnFamily, rowKey)
        .putEmptyColumn(column, ttl.map(int2Integer(_)).orNull[java.lang.Integer])
    }

    def ++=(path: (K, C))(implicit @implicitNotFound("Mutation batch must be implicitly provided!") mutationBatch: MutationBatch): ColumnListMutation[C] = {
      path match {
        case (rowKey, column) => ++=(rowKey, column, None)(mutationBatch)
      }
    }


      /**
     * Batch increment counter column
     *
     * @param rowKey
     * @param column
     * @param value
     * @param mutationBatch
     * @return
     */

    def ^^=(rowKey: K, column: C, value: Long)(implicit @implicitNotFound("Mutation batch must be implicitly provided!") mutationBatch: MutationBatch): ColumnListMutation[C] = {
      mutationBatch.withRow(columnFamily, rowKey).incrementCounterColumn(column, value)
    }

    def ^^=(path: ((K, C), Long))(implicit @implicitNotFound("Mutation batch must be implicitly provided!") mutationBatch: MutationBatch): ColumnListMutation[C] = {
      path match {
        case ((rowKey, column), value) => {
          ^^=(rowKey, column, value)(mutationBatch)
        }
      }
    }


    /**
     * Batch delete column
     *
     * @param rowKey
     * @param column
     * @param mutationBatch
     * @return
     */

    def --=(rowKey: K, column: C)(implicit @implicitNotFound("Mutation batch must be implicitly provided!") mutationBatch: MutationBatch): ColumnListMutation[C] = {
      mutationBatch.withRow(columnFamily, rowKey)
        .deleteColumn(column)
    }

    def --=(path: (K, C))(implicit @implicitNotFound("Mutation batch must be implicitly provided!") mutationBatch: MutationBatch): ColumnListMutation[C] = {
      path match {
        case (rowKey, column) => --=(rowKey, column)(mutationBatch)
      }
    }
  }

  implicit class QueryableColumnFamily[K, C](val columnFamily: ColumnFamily[K, C]) {

    /**
     * Fetch whole row.
     *
     * @param rowKey
     * @param keyspace
     * @return
     */

    def apply(rowKey: K)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): RowQuery[K, C] = {
      keyspace.prepareQuery(columnFamily)
        .getRow(rowKey)
    }


    /**
     * Fetch a single column from a row.
     *
     * @param rowKey
     * @param column
     * @param keyspace
     * @return
     */

    def apply(rowKey: K, column: C)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): ColumnQuery[C] = {
      keyspace.prepareQuery(columnFamily)
        .getRow(rowKey)
        .getColumn(column)
    }

    /**
     * Fetch a single column from a row using a path syntax.
     *
     * @param path
     * @param keyspace
     * @return
     */

    def apply(path: (K, C))(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): ColumnQuery[C] = path match {
      case (rowKey, column) => apply(rowKey, column)(keyspace)
    }

    /**
     * Column slice query.
     *
     * @param rowKey
     * @param columns
     * @param keyspace
     * @return
     */

    def apply(rowKey: K, columns: Iterable[C])(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): RowQuery[K, C] = {
      keyspace.prepareQuery(columnFamily)
        .getKey(rowKey)
        .withColumnSlice(columns)
    }

    /**
     * Column slice query using a path syntax.
     *
     * @param path
     * @param keyspace
     * @return
     */

    def apply(path: (K, Iterable[C]))(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): RowQuery[K, C] = path match {
      case (rowKey, columns) => apply(rowKey, columns)(keyspace)
    }

    /**
     * Row slice query.
     *
     * @param rowKeys
     * @param keyspace
     * @return
     */
    def apply(rowKeys: Iterable[K])(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): RowSliceQuery[K, C] = {
      keyspace.prepareQuery(columnFamily)
        .getKeySlice(rowKeys)
    }

    /**
     * Row and column slice query.
     *
     * @param rowKeys
     * @param columns
     * @param keyspace
     * @return
     */

    def apply(rowKeys: Iterable[K], columns: Iterable[C])(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): RowSliceQuery[K, C] = {
      keyspace.prepareQuery(columnFamily)
        .getKeySlice(rowKeys)
        .withColumnSlice(columns)
    }

    /**
     * Row and columns slice query using a path syntax.
     *
     * @param path
     * @param keyspace
     * @return
     */
    def apply(path: (Iterable[K], Iterable[C]))(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): RowSliceQuery[K, C] = path match {
      case (rowKeys, columns) => apply(rowKeys, columns)(keyspace)
    }


    /**
     * Column range query.
     *
     * @param rowKey
     * @param columnRange
     * @param keyspace
     * @param typeTagK
     * @param typeTagC
     * @return
     */
    def apply(rowKey: K, columnRange: RangeQuery[C])(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, typeTagK: TypeTag[K], typeTagC: TypeTag[C]): RowQuery[K, C] = {
      keyspace.prepareQuery(columnFamily).getRow(rowKey)
        .withColumnRange(columnRange.fromOrNull, columnRange.toOrNull, columnRange.reverse, columnRange.limitOrNull)
    }


    /**
     * Column range query using path syntax.
     *
     * @param path
     * @param keyspace
     * @param typeTagK
     * @param typeTagC
     * @return
     */
    def apply(path: (K, RangeQuery[C]))(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, typeTagK: TypeTag[K], typeTagC: TypeTag[C]): RowQuery[K, C] = path match {
      case (rowKey, columnRange) => {
        apply(rowKey, columnRange)(keyspace, typeTagK, typeTagC)
      }
    }

    /**
     * Row slice and column range query.
     *
     * @param rowKeys
     * @param columnRange
     * @param keyspace
     * @param typeTagK
     * @param typeTagC
     * @return
     */
    def apply(rowKeys: Iterable[K], columnRange: RangeQuery[C])(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, typeTagK: TypeTag[K], typeTagC: TypeTag[C]): RowSliceQuery[K, C] = {
      keyspace.prepareQuery(columnFamily).getRowSlice(rowKeys)
        .withColumnRange(columnRange.fromOrNull, columnRange.toOrNull, columnRange.reverse, columnRange.limitOrNull)
    }

    /**
     * Row slice and column range query using path syntax.
     *
     * @param path
     * @param keyspace
     * @param typeTagK
     * @param typeTagC
     * @return
     */
    def apply(path: (Iterable[K], RangeQuery[C]))(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, typeTagK: TypeTag[K], typeTagC: TypeTag[C]): RowSliceQuery[K, C] = path match {
      case (rowKeys, columnRange) => {
        apply(rowKeys, columnRange)(keyspace, typeTagK, typeTagC)
      }
    }

    def foreach(function: Row[K, C] => Boolean)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): AllRowsReader.Builder[K, C] = {
      new AllRowsReader.Builder(keyspace, columnFamily)
        .forEachRow(forEachRowWrapper(function))
    }

    def foreachPage(function: Rows[K, C] => Boolean)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace): AllRowsReader.Builder[K, C] = {
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

  implicit class MainteinableColumnFamily[K, C](columnFamily: ColumnFamily[K, C]) {

    /**
     * Craete column family if one does not exist.
     *
     * @param properties
     * @param keyspace
     * @return
     */

    def create(properties: Map[String, AnyRef])(implicit keyspace: Keyspace): Boolean = {
      keyspace.describeKeyspace().getColumnFamilyList.map(_.getName).contains(columnFamily.getName) match {
        case true => false
        case false => {
          keyspace.createColumnFamily(columnFamily, properties)
          true
        }
      }
    }

    def create(properties: (String, AnyRef)*)(implicit keyspace: Keyspace): Boolean = {
      create(properties.toMap)(keyspace)
    }

    /**
     * Truncate column family
     *
     * @param keyspace
     * @return
     */

    def truncate(implicit keyspace: Keyspace): OperationResult[Void] = {
      keyspace.truncateColumnFamily(columnFamily)
    }
  }
}

