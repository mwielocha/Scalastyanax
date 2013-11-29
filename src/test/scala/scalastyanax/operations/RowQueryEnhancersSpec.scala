package scalastyanax.operations

import org.specs2.mutable.Specification
import scalastyanax.Cassandra
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnList
import scala.util.Failure
import scala.util._

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 26.11.2013
 * Time: 22:18
 * To change this template use File | Settings | File Templates.
 */
class RowQueryEnhancersSpec extends Specification {

  sequential

  import scalastyanax.Scalastyanax._

  "RowQuery" should {

    "perform a query" in new Cassandra {

      (columnFamily += ("RowQueryTestKey" -> "RowQueryTestColumn" -> "RowQueryTestValue")).execute

      val batch = keyspace.mutate { implicit batch =>
        columnFamily ++= ("Row" -> "Column" -> "Value")
        columnFamily ++= ("Row 2" -> "Column 2" -> "Value 2")
      }

      keyspace.mutate(batch) { implicit batch =>
        columnFamily ++= ("Row 3" -> "Column 3" -> "Value 3")
        columnFamily ++= ("Row 4" -> "Column 4" -> "Value 4")
      }.execute


      columnFamily("RowQueryTestKey").get {
        case Success(result) => result.
        case Failure(t) =>
      }

      1 === 1
    }
  }
}
