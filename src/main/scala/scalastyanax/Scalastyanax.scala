package scalastyanax

import scalastyanax.operations.{RowSliceQueryEnhancers, RowQueryEnhancers, ColumnFamilyEnhancers, KeyspaceEnhancers}
import scalastyanax.result.{RowsEnhancers, ColumnListEnhancers}

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 26.11.2013
 * Time: 21:41
 * To change this template use File | Settings | File Templates.
 */
object Scalastyanax extends KeyspaceEnhancers
  with ColumnFamilyEnhancers
  with RowQueryEnhancers
  with RowSliceQueryEnhancers
  with RowsEnhancers
  with RangeQueryBuilder
