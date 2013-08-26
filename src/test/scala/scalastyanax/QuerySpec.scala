package scalastyanax

import org.specs2.mutable.Specification
import org.specs2._
import Query._
import scala.collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 19.08.2013
 * Time: 22:03
 * To change this template use File | Settings | File Templates.
 */
class QuerySpec extends Specification {

  sequential

  "Scalastyanax" should {

    "answer simple row question" in new Cassandra {

      query.one("D").one("D") match {
        case Success(c) => c.as[String] mustEqual Some("D")
        case Failure(t) => failure
      }
    }

    "answer ranged column question" in new Cassandra {

      query.one("D").range(Range("D", 20)).execute match {
        case Success(columns) => columns.stream.flatMap(_.as[String]) mustEqual keys.dropWhile(_ != "D")
        case Failure(_) => failure
      }
    }

    "answer reversed ranged column question" in new Cassandra {

      query.one("D").range(Range("D", 20).reversed).execute match {
        case Success(columns) => columns.stream.flatMap(_.as[String]) mustEqual keys.reverse.dropWhile(_ != "D")
        case Failure(_) => failure
      }
    }

    "answer ranged (builded) column question" in new Cassandra {

      query.one("D").range(Range.from("D").take(30)).execute match {
        case Success(columns) => columns.stream.flatMap(_.as[String]) mustEqual keys.dropWhile(_ != "D")
        case Failure(_) => failure
      }
    }

    "answer ranged (builded with dsl) column question" in new Cassandra {

      query <-? "D" <~? Range.from("D").take(30) execute match {
        case Success(columns) => columns.stream.flatMap(_.as[String]) mustEqual keys.dropWhile(_ != "D")
        case Failure(_) => failure
      }
    }

    "answer sliced column question" in new Cassandra {

      query.one("D").slice(Seq("D", "A", "K")).execute match {
        case Success(columns) => columns.stream.flatMap(_.as[String]) mustEqual Seq("A", "D", "K")
        case Failure(_) => failure
      }
    }

    "answer (builded with dsl) sliced column question" in new Cassandra {

      query <-? ("D") </? Seq("D", "A", "K") execute match {
        case Success(columns) => columns.stream.flatMap(_.as[String]) mustEqual Seq("A", "D", "K")
        case Failure(_) => failure
      }
    }

    "answer sliced rows question" in new Cassandra {

      query.slice(Seq("D", "A", "K")).range(Range("D")).execute match {
        case Success(rows) => rows.flatten.flatMap(_.as[String]) mustEqual Seq("D", "D", "D")
        case Failure(_) => failure
      }
    }

    "answer sliced rows and columns question" in new Cassandra {

      query.slice(Seq("D", "A", "K")).slice(Seq("A", "D", "K")).execute match {
        case Success(rows) => rows.flatten.flatMap(_.as[String]) mustEqual {
          scala.Range(0, 3).map(x => Seq("A", "D", "K")).toSeq.flatten
        }
        case Failure(_) => failure
      }
    }
  }
}