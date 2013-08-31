package scalastyanax

import com.netflix.astyanax.{query => astxq, model => astxm, Keyspace}
import com.netflix.{astyanax => astxr}
import scalastyanax.JavaInteropt._

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 31.08.2013
 * Time: 12:40
 * To change this template use File | Settings | File Templates.
 */
object Mutation {

  def mutate[K, C](row: K, column: C)(implicit keyspace: Keyspace, columnFamily: astxm.ColumnFamily[K, C]) = {
    ColumnMutaion(keyspace.prepareColumnMutation(columnFamily, row, column))
  }

}

case class ColumnMutaion(astx: astxr.ColumnMutation) {

  def put(implicit ttl: Option[Int]): Execution[Void] = {
    Execution.wrap(astx.putEmptyColumn(ttl.getOrElseNull).execute.getResult)
  }

  def put[V](value: V)(implicit ttl: Option[Int]) = {
    Execution.wrap {
      (value match {
        case clazz if (clazz == classOf[String]) => astx.putValue(value.asInstanceOf[String], ttl.getOrElseNull)
      }).execute().getResult
    }
  }
}



