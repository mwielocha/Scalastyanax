package scalastyanax.operations

import com.netflix.astyanax.query.RowSliceQuery
import scala.util.Try
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.{Rows, ColumnList}

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 01.12.2013
 * Time: 13:49
 * To change this template use File | Settings | File Templates.
 */

object RowSliceQueryImplicits extends RowSliceQueryEnhancers

trait RowSliceQueryEnhancers {

  implicit class EnhancedRowSliceQuery[K, C](val rowSliceQuery: RowSliceQuery[K, C]) {

    def get: Try[OperationResult[Rows[K, C]]] = Try(rowSliceQuery.execute())

  }
}
