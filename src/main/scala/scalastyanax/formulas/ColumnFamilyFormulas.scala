package scalastyanax.formulas

import com.netflix.astyanax.model.{ColumnFamily, Row, Column}
import scala.annotation.{tailrec, implicitNotFound}
import com.netflix.astyanax.Keyspace
import scalastyanax.operations.{ColumnFamilyEnhancers, RowQueryImplicits}
import com.netflix.astyanax.recipes.reader.AllRowsReader
import java.lang
import scalastyanax.RangeQuery
import scala.util.{Success, Failure}
import scala.collection.JavaConversions._
import RowQueryImplicits._

/**
 * author mikwie
 *
 */
trait ColumnFamilyFormulas {
  self: ColumnFamilyEnhancers =>

  implicit class TraversableColumnFamily[K, C](val columnFamily: ColumnFamily[K, C]) {

    def foreachWithPaging(rowKey: K, function: Iterable[Column[C]] => Unit, pageSize: Int)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, manifestK: Manifest[K], manifestC: Manifest[C]) = {

      @tailrec
      def _function(columns: Iterable[Column[C]]): Boolean = {

        columns.size match {
          case 0 => true
          case otherwise => {
            val from = columns.map(_.getName).lastOption
            function(columns)
            columnFamily(rowKey -> RangeQuery[C](from, None, Some(pageSize), false)).get match {
              case Failure(e) => false
              case Success(result) => {
                _function(result.getResult.drop(1))
              }
            }
          }
        }
      }

      columnFamily(rowKey -> RangeQuery[C](None, None, Some(pageSize), false)).get match {
        case Failure(e) => false
        case Success(result) => {
          _function(result.getResult)
        }
      }
    }

    def foreachWithPaging(function: (K, Iterable[Column[C]]) => Unit, pageSize: Int)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, manifestK: Manifest[K], manifestC: Manifest[C]) = {

      new AllRowsReader.Builder(keyspace, columnFamily)
        .withColumnRange(null.asInstanceOf[C], null.asInstanceOf[C], false, pageSize)
        .forEachRow(new com.google.common.base.Function[Row[K, C], java.lang.Boolean]() {

        def apply(input: Row[K, C]): lang.Boolean = {

          val rowKey = input.getKey
          val columns = input.getColumns

          @tailrec
          def _function(columns: Iterable[Column[C]]): Boolean = {

            columns.size match {
              case 0 => true
              case otherwise => {
                val from = columns.map(_.getName).lastOption
                function(rowKey, columns)

                columnFamily(rowKey -> RangeQuery[C](from, None, Some(pageSize), false)).get match {
                  case Failure(e) => false
                  case Success(result) => {
                    _function(result.getResult.drop(1))
                  }
                }
              }
            }
          }

          _function(columns)
        }
      })
    }
  }
}
