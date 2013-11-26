package scalastyanax.operations

import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.{MutationBatch, Keyspace}

/**
 * author mikwie
 *
 */
object ColumnFamilyOps {

  implicit def enhanceColumnFamily[K, C](columnFamily: ColumnFamily[K, C]) = new EnhancedColumnFamily[K, C](columnFamily)

}

class EnhancedColumnFamily[K, C](val columnFamily: ColumnFamily[K, C]) {


  def putValue(rowKey: K, column: C, value: String, ttl: Option[Int] = None)(implicit keyspace: Keyspace) = {
    keyspace.prepareColumnMutation(columnFamily, rowKey, column).putValue(value, ttl.getOrElse(null.asInstanceOf[Int]))
  }

  def +=(rowKey: K, column: C, value: String, ttl: Option[Int] = None)(implicit mutationBatch: MutationBatch) = {
    mutationBatch.withRow(columnFamily, rowKey).putColumn(column, value, ttl.getOrElse(null.asInstanceOf[Int]))
  }


}
