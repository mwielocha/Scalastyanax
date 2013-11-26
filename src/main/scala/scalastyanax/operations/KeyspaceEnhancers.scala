package scalastyanax.operations

import com.netflix.astyanax.{MutationBatch, Keyspace}

/**
 * author mikwie
 *
 */

object KeyspaceImplicits extends KeyspaceEnhancers

trait KeyspaceEnhancers {

  implicit class MutableKeyspace(val keyspace: Keyspace) {

    def mutate(mutation: MutationBatch => Unit): MutationBatch = {
      mutate(keyspace.prepareMutationBatch())(mutation)
    }

    def mutate(mutationBatch: MutationBatch)(mutation: MutationBatch => Unit): MutationBatch = {
      mutation(mutationBatch)
      mutationBatch
    }
  }
}

