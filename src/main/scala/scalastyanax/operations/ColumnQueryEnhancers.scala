package scalastyanax.operations

import com.netflix.astyanax.query.ColumnQuery
import scala.util.Try
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.{Column, ColumnList}

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 01.12.2013
 * Time: 15:53
 * To change this template use File | Settings | File Templates.
 */
object ColumnQueryImplicits extends ColumnQueryEnhancers

trait ColumnQueryEnhancers {

  implicit class EnhancedColumnQuery[C](val columnQuery: ColumnQuery[C]) {

    def get: Try[OperationResult[Column[C]]] = Try(columnQuery.execute())
  }
}
