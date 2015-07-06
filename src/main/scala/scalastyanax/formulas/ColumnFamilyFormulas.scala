package scalastyanax.formulas

import com.netflix.astyanax.model.{ColumnFamily, Row, Column}
import com.netflix.astyanax.partitioner.Partitioner
import scala.annotation.{tailrec, implicitNotFound}
import com.netflix.astyanax.Keyspace
import scalastyanax.operations.{ColumnFamilyEnhancers, RowQueryImplicits}
import com.netflix.astyanax.recipes.reader.AllRowsReader
import java.lang
import scalastyanax.RangeQuery
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._
import RowQueryImplicits._

/**
 * author mikwie
 *
 */
trait ColumnFamilyFormulas {
  self: ColumnFamilyEnhancers =>

  implicit class TraversableColumnFamily[K, C](val columnFamily: ColumnFamily[K, C]) {

    def foreachRowKey(function: K => Unit)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, manifestK: Manifest[K], manifestC: Manifest[C]) = {
      new AllRowsReader.Builder(keyspace, columnFamily)
        .withPartitioner(null.asInstanceOf[Partitioner])
        .withColumnRange(null.asInstanceOf[C], null.asInstanceOf[C], false, 0)
        .forEachRow(new com.google.common.base.Function[Row[K, C], java.lang.Boolean]() {

        def apply(input: Row[K, C]): lang.Boolean = {
          Try(function(input.getKey)) match{
            case Success(_) => true
            case Failure(_) => false
          }
        }
      })
    }

    def foreachWithPaging(rowKey: K, function: Iterable[Column[C]] => Unit, pageSize: Int)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, manifestK: Manifest[K], manifestC: Manifest[C]) = {

      @tailrec
      def _function(columns: Iterable[Column[C]]): Boolean = {

        columns.size match {
          case 0 => true
          case otherwise =>
            val from = columns.map(_.getName).lastOption
            function(columns)
            columnFamily(rowKey -> RangeQuery[C](from, None, Some(pageSize), reverse = false)).get match {
              case Failure(e) => false
              case Success(result) =>
                _function(result.getResult.drop(1))
            }
        }
      }

      columnFamily(rowKey -> RangeQuery[C](None, None, Some(pageSize), reverse = false)).get match {
        case Failure(e) => false
        case Success(result) =>
          _function(result.getResult)
      }
    }

    def foldLeftWithPaging[A](zero: A)(rowKey: K, function: (A, Iterable[Column[C]]) => A, pageSize: Int)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, manifestK: Manifest[K], manifestC: Manifest[C]): A = {

      @tailrec
      def _iteration(acc: A, columns: Iterable[Column[C]]): A = {

        columns.size match {
          case 0 => acc
          case otherwise =>
            val from = columns.map(_.getName).lastOption
            val partition = function(acc, columns)
            columnFamily(rowKey -> RangeQuery[C](from, None, Some(pageSize), reverse = false)).get match {
              case Failure(e) => throw e
              case Success(result) =>
                _iteration(partition, result.getResult.drop(1))
            }
        }
      }

      columnFamily(rowKey -> RangeQuery[C](None, None, Some(pageSize), reverse = false)).get match {
        case Failure(e) => throw e
        case Success(result) =>
          _iteration(zero, result.getResult)
      }
    }

    def foreachWithPaging(function: (K, Iterable[Column[C]]) => Unit, pageSize: Int)(implicit @implicitNotFound("Keyspace must be implicitly provided!") keyspace: Keyspace, manifestK: Manifest[K], manifestC: Manifest[C]) = {

      new AllRowsReader.Builder(keyspace, columnFamily)
        .withColumnRange(null.asInstanceOf[C], null.asInstanceOf[C], false, pageSize)
        .withPartitioner(null.asInstanceOf[Partitioner])
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

                columnFamily(rowKey -> RangeQuery[C](from, None, Some(pageSize), reverse = false)).get match {
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
