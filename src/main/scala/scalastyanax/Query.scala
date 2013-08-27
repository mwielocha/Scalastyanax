package scalastyanax

import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.model.ColumnFamily
import scala.collection.JavaConversions._
import com.netflix.astyanax.{query => astxq}
import com.netflix.astyanax.{model => astxm}

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 19.08.2013
 * Time: 21:22
 * To change this template use File | Settings | File Templates.
 */

object Query {

  def query[K, C](implicit context: QueryContext[K, C]): ColumnFamilyQuery[K, C] = {
    context.prepareQuery
  }
}

case class ColumnFamilyQuery[K, C](astx: astxq.ColumnFamilyQuery[K, C]) {

  def one(key: K): RowQuery[K, C] = {
    RowQuery(astx.getRow(key))
  }

  def <-? (key: K) = one(key)

  def slice(keys: Iterable[K]): RowSliceQuery[K, C] = {
    RowSliceQuery(astx.getRowSlice(keys))
  }

  def slice(keys: K*): RowSliceQuery[K, C] = {
    slice(keys.toIterable)
  }
}

case class RowQuery[K, C](astx: astxq.RowQuery[K, C]) {

  def one(key: C) = {
    Execution.wrap(Column(astx.getColumn(key).execute().getResult))
  }

  def <-? (key: C) = one(key)

  def range(selector: Range[C]) = {
    RowQuery(astx.withColumnRange(
      selector.firstOrNull,
      selector.lastOrNull,
      selector.reverse,
      selector.limit))
  }

  def <~? (selector: Range[C]): RowQuery[K, C] = range(selector)

  def slice(selector: Iterable[C]): RowQuery[K, C] = {
    RowQuery(astx.withColumnSlice(selector))
  }

  def slice(selector: C*): RowQuery[K, C] = {
    slice(selector.toIterable)
  }

  def </? (selector: Iterable[C]): RowQuery[K, C] = slice(selector)

  def execute = {
    Execution.wrap(Columns(astx.execute().getResult))
  }

  def !!! = execute
}

case class RowSliceQuery[K, C](astx: astxq.RowSliceQuery[K, C]) {

  def range(selector: Range[C]): RowSliceQuery[K, C] = {
    RowSliceQuery(astx.withColumnRange(
      selector.firstOrNull,
      selector.lastOrNull,
      selector.reverse,
      selector.limit))
  }

  def <~? (selector: Range[C]): RowSliceQuery[K, C] = range(selector)

  def slice(selector: Iterable[C]): RowSliceQuery[K, C] = {
    RowSliceQuery(astx.withColumnSlice(selector))
  }

  def slice(selector: C*): RowSliceQuery[K, C] = {
    slice(selector.toIterable)
  }

  def </? (selector: Iterable[C]): RowSliceQuery[K, C] = slice(selector)

  def execute = {
    Execution.wrap(Rows(astx.execute().getResult))
  }

  def !!! = execute
}

trait Execution[R]

case class Success[R](data: R) extends Execution[R]

case class Failure[R](throwable: Throwable) extends Execution[R]

object Execution {

  def wrap[R](execution: => R): Execution[R] = {
    try {
      Success(execution)
    } catch {
      case throwable: Throwable => Failure(throwable)
    }
  }
}
