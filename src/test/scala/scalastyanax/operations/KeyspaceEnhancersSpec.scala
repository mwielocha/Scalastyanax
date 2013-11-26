package scalastyanax.operations

import org.specs2.mutable.Specification
import scalastyanax.Cassandra

/**
 * author mikwie
 *
 */
class KeyspaceEnhancersSpec extends Specification {

  sequential

  import scalastyanax.Scalastyanax._

  "Keyspace Ops" should {

    "performa a batch mutation" in new Cassandra {

      keyspace.mutate(implicit mutationBatch => {

        1 to 10 foreach { index =>
          columnFamily ++= (s"Row #$index" -> s"Column #$index" -> s"Value #$index")
        }
      }).execute

      1 === 1
    }
  }
}
