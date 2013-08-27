package scalastyanax

import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.model.ConsistencyLevel
import com.netflix.astyanax.retry.{RunOnce, RetryPolicy}
import com.netflix.astyanax.{query => astxq}
import com.netflix.astyanax.{model => astxm}

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 27.08.2013
 * Time: 20:30
 * To change this template use File | Settings | File Templates.
 */
case class QueryContext[K, C](keyspace: Keyspace,
                              columnFamily: astxm.ColumnFamily[K, C],
                              consistencyLevel: ConsistencyLevel = ConsistencyLevel.CL_ONE,
                              retryPolicy: RetryPolicy = RunOnce.get()) {

  def withConsistencyLevel(consistencyLevel: ConsistencyLevel) = {
    copy(consistencyLevel = consistencyLevel)
  }

  def withRetryPolicy(retryPolicy: RetryPolicy) = {
    copy(retryPolicy = retryPolicy)
  }

  private[scalastyanax] def prepareQuery: ColumnFamilyQuery[K, C] = {
    ColumnFamilyQuery(keyspace.prepareQuery(columnFamily)
      .setConsistencyLevel(consistencyLevel)
      .withRetryPolicy(retryPolicy)
    )
  }
}
