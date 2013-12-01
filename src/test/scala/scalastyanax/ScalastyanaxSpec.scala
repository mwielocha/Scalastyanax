package scalastyanax

import org.specs2.mutable.Specification
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
class ScalastyanaxSpec extends Specification {

  sequential

  import scalastyanax.Scalastyanax._

  "Column Family" should {

    "perform a single value query" in new CassandraContext {

      (columnFamily += ("RowQueryTestKey" -> "RowQueryTestColumn" -> "RowQueryTestValue")).execute

      val queryResult: Option[String] = columnFamily("RowQueryTestKey").get match {
        case Success(result) => result.getResult()[String]("RowQueryTestColumn")
        case Failure(t) => None
      }

      queryResult === Some("RowQueryTestValue")
    }

    "perform a list query" in new CassandraContext {

      val batch = keyspace.newMutationBatch { implicit batch =>
        columnFamily ++= ("Row 1" -> "Column 1" -> "Value 1")
        columnFamily ++= ("Row 1" -> "Column 2" -> "Value 2")
        columnFamily ++= ("Row 1" -> "Column 3" -> "Value 3")
        columnFamily ++= ("Row 1" -> "Column 4" -> "Value 4")
      }.execute

      val values = columnFamily("Row 1").get match {
        case Success(result) => result.getResult.values[String]
        case Failure(t) => Nil
      }

      values === Seq("Value 1", "Value 2", "Value 3", "Value 4")

    }

    "perform a list query with value mapping" in new CassandraContext {

      val batch = keyspace.newMutationBatch { implicit batch =>
        columnFamily ++= ("Row 1" -> "Column 1" -> "Value 1")
        columnFamily ++= ("Row 1" -> "Column 2" -> "Value 2")
        columnFamily ++= ("Row 1" -> "Column 3" -> "Value 3")
        columnFamily ++= ("Row 1" -> "Column 4" -> "Value 4")
      }.execute

      val values: Iterable[Int] = columnFamily("Row 1").get match {
        case Success(result) => result.getResult.flatMapValues[String, Int](_.replace("Value ", "").toInt)
        case Failure(t) => Nil
      }

      values === Seq(1, 2, 3, 4)

    }

    "perform a column slice query" in new CassandraContext {

      val batch = keyspace.newMutationBatch { implicit batch =>
        columnFamily ++= ("Row 1" -> "Column 1" -> "Value 1")
        columnFamily ++= ("Row 1" -> "Column 2" -> "Value 2")
        columnFamily ++= ("Row 1" -> "Column 3" -> "Value 3")
        columnFamily ++= ("Row 1" -> "Column 4" -> "Value 4")
      }.execute

      val values: Iterable[String] = columnFamily("Row 1", Seq("Column 1", "Column 4")).get match {
        case Success(result) => result.getResult.values[String]
        case Failure(t) => Nil
      }

      values === Seq("Value 1", "Value 4")
    }

    "perform a column slice query using a path syntax" in new CassandraContext {

      val batch = keyspace.newMutationBatch { implicit batch =>
        columnFamily ++= ("Row 1" -> "Column 1" -> "Value 1")
        columnFamily ++= ("Row 1" -> "Column 2" -> "Value 2")
        columnFamily ++= ("Row 1" -> "Column 3" -> "Value 3")
        columnFamily ++= ("Row 1" -> "Column 4" -> "Value 4")
      }.execute

      val values: Iterable[String] = columnFamily("Row 1" -> Seq("Column 1", "Column 4")).get match {
        case Success(result) => result.getResult.values[String]
        case Failure(t) => Nil
      }

      values === Seq("Value 1", "Value 4")
    }

    "perform a row and column slice query using a path syntax" in new CassandraContext {

      val batch = keyspace.newMutationBatch { implicit batch =>
        columnFamily ++= ("Row 1" -> "Column 1" -> "Value 1")
        columnFamily ++= ("Row 1" -> "Column 2" -> "Value 2")
        columnFamily ++= ("Row 2" -> "Column 3" -> "Value 3")
        columnFamily ++= ("Row 3" -> "Column 4" -> "Value 4")
      }.execute

      val values: Map[String, Iterable[String]] = columnFamily(Seq("Row 1", "Row 2") -> Seq("Column 1", "Column 2", "Column 3", "Column 4")).get match {
        case Success(result) => result.getResult.toValueMap[String]
        case Failure(t) => Map.empty
      }

      values === Map("Row 1" -> Seq("Value 1", "Value 2"), "Row 2" -> Seq("Value 3"))
    }

    "perform a row and column slice query with fattened values using a path syntax" in new CassandraContext {

      val batch = keyspace.newMutationBatch { implicit batch =>
        columnFamily ++= ("Row 1" -> "Column 1" -> "Value 1")
        columnFamily ++= ("Row 1" -> "Column 2" -> "Value 2")
        columnFamily ++= ("Row 2" -> "Column 3" -> "Value 3")
        columnFamily ++= ("Row 3" -> "Column 4" -> "Value 4")
      }.execute

      val values: Iterable[String] = columnFamily(Seq("Row 1", "Row 2") -> Seq("Column 1", "Column 2", "Column 3", "Column 4")).get match {
        case Success(result) => result.getResult.flatValues[String]
        case Failure(t) => Nil
      }

      values.toSeq.sorted === Seq("Value 1", "Value 2", "Value 3")
    }

    "perform a range query" in new CassandraContext {

      val batch = keyspace.newMutationBatch { implicit batch =>
        columnFamily ++= ("Row" -> "Column 1" -> "Value 1")
        columnFamily ++= ("Row" -> "Column 2" -> "Value 2")
        columnFamily ++= ("Row" -> "Column 3" -> "Value 3")
        columnFamily ++= ("Row" -> "Column 4" -> "Value 4")
        columnFamily ++= ("Row" -> "Column 5" -> "Value 5")
        columnFamily ++= ("Row" -> "Column 6" -> "Value 6")
        columnFamily ++= ("Row" -> "Column 7" -> "Value 7")
        columnFamily ++= ("Row" -> "Column 8" -> "Value 8")
      }.execute

      val values: Iterable[String] = columnFamily("Row" -> (from("Column 2") to("Column 8") take 4)).get match {
        case Success(result) => result.getResult.values[String]
        case Failure(t) => Nil
      }

      values.toSeq.sorted === Seq("Value 2", "Value 3", "Value 4", "Value 5")
    }

    "perform a reversed range query" in new CassandraContext {

      val batch = keyspace.newMutationBatch { implicit batch =>
        columnFamily ++= ("Row" -> "Column 1" -> "Value 1")
        columnFamily ++= ("Row" -> "Column 2" -> "Value 2")
        columnFamily ++= ("Row" -> "Column 3" -> "Value 3")
        columnFamily ++= ("Row" -> "Column 4" -> "Value 4")
        columnFamily ++= ("Row" -> "Column 5" -> "Value 5")
        columnFamily ++= ("Row" -> "Column 6" -> "Value 6")
        columnFamily ++= ("Row" -> "Column 7" -> "Value 7")
        columnFamily ++= ("Row" -> "Column 8" -> "Value 8")
      }.execute

      val values: Iterable[String] = columnFamily("Row" -> (from("Column 6") take 4 reversed)).get match {
        case Success(result) => result.getResult.values[String]
        case Failure(t) => Nil
      }

      values.toSeq.sorted === Seq("Value 3", "Value 4", "Value 5", "Value 6")
    }
  }
}
