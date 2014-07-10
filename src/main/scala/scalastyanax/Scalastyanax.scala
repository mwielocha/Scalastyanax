package scalastyanax

import scalastyanax.operations._
import scalastyanax.result.{RowsEnhancers, ColumnListEnhancers}
import scalastyanax.model.CompositeEnhancers
import scalastyanax.formulas.ColumnFamilyFormulas

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 26.11.2013
 * Time: 21:41
 * To change this template use File | Settings | File Templates.
 */
object Scalastyanax extends KeyspaceEnhancers
  with ColumnFamilyEnhancers
  with ColumnFamilyFormulas
  with RowQueryEnhancers
  with RowSliceQueryEnhancers
  with RowsEnhancers
  with RangeQueryBuilder
  with ColumnQueryEnhancers
  with AllRowsQueryEnhancers
  with CompositeEnhancers
