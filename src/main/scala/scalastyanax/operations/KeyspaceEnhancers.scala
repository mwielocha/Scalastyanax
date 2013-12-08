package scalastyanax.operations

import com.netflix.astyanax.{MutationBatch, Keyspace}
import com.netflix.astyanax.model.ColumnFamily

/**
 * author mikwie
 *
 */

object KeyspaceImplicits extends KeyspaceEnhancers

trait KeyspaceEnhancers {

  implicit class MutableKeyspace(val keyspace: Keyspace) {

    def newMutationBatch(mutation: MutationBatch => Unit): MutationBatch = {
      withMutationBatch(keyspace.prepareMutationBatch())(mutation)
    }

    def withMutationBatch(mutationBatch: MutationBatch)(mutation: MutationBatch => Unit): MutationBatch = {
      mutation(mutationBatch)
      mutationBatch
    }
  }
}

