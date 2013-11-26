package scalastyanax.operations

import org.specs2.mutable.Specification
import scalastyanax.Cassandra
import com.netflix.astyanax.connectionpool.OperationResult
import com.netflix.astyanax.model.ColumnList

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

      columnFamily("RowQueryTestKey").perform[String]({
        case Right(result) => result.getResult
        case Left(t) => t.getMessage
      })

      1 === 1
    }
  }
}
