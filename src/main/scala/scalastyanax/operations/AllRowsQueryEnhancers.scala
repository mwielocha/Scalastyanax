package scalastyanax.operations

import com.netflix.astyanax.query.AllRowsQuery
import scala.util.Try
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.Rows
import com.netflix.astyanax.connectionpool.exceptions.{ConnectionException, OperationException}
import com.netflix.astyanax.RowCallback
import scalastyanax.RangeQuery

/**
 * author mikwie
 *
 */

object AllRowsQueryImplicits extends AllRowsQueryEnhancers

trait AllRowsQueryEnhancers {

  implicit class EnhancedAllRowsQuery[K, C](val allRowsQuery: AllRowsQuery[K, C]) {

    def get: Try[OperationResult[Rows[K, C]]] = {
      Try(allRowsQuery.execute)
    }

    def withColumnRange(range: RangeQuery[C]) = {
      allRowsQuery.withColumnRange(range.fromOrNull, range.toOrNull, range.reverse, range.limitOrNull)
    }

    def getWithCallback(callback: Either[ConnectionException, Rows[K, C]] => Boolean) = {
      allRowsQuery.executeWithCallback(RowCallbackWrapper(callback))
    }

    private case class RowCallbackWrapper[K, C](callback: Either[ConnectionException, Rows[K, C]] => Boolean) extends RowCallback[K, C] {
      def success(rows: Rows[K, C]): Unit = {
        callback(Right(rows))
      }

      def failure(e: ConnectionException): Boolean = {
        callback(Left(e))
      }
    }
  }
}
