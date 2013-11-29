package scalastyanax.operations

import com.netflix.astyanax.query.RowQuery
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnList
import scalastyanax.common.ExecutorHelper
import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 26.11.2013
 * Time: 22:06
 * To change this template use File | Settings | File Templates.
 */

object RowQueryImplicits extends RowQueryEnhancers

trait RowQueryEnhancers extends ExecutorHelper {

  implicit class EnhancedRowQuery[K, C](val rowQuery: RowQuery[K, C]) {

    def get: Try = Try(rowQuery.execute())
  }
}
