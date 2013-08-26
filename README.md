Scalastyanax
============

Astyanax scala wrapper.

Usage:

Simple one row, one column fetch:

```scala
import scalastyanax.Query._

  query.one("D").one("D") match {
        case Success(c) => c.as[String]
        case Failure(t) => _
      }
```
      
Column range query:

```scala
  query.one("D").range(Range("D", 20)).execute match {
        case Success(columns) => columns.stream.flatMap(_.as[String])
        case Failure(_) => _
      }
```
      
Range builder query:

```scala
  query.one("D").range(Range.from("D").take(30)).execute match {
        case Success(columns) => columns.stream.flatMap(_.as[String])
        case Failure(_) => _
      }
```
      
Range dsl query:

```scala
  query <-? "D" <~? Range.from("D").take(30) execute match {
        case Success(columns) => columns.stream.flatMap(_.as[String])
        case Failure(_) => _
      }
```

Column slice query:

```scala
  query.slice(Seq("D", "A", "K")).range(Range("D")).execute match {
        case Success(rows) => rows.flatten.flatMap(_.as[String])
        case Failure(_) => _
      }
```

Column slice dsl query:

```scala
  query <-? ("D") </? Seq("D", "A", "K") execute match {
        case Success(columns) => columns.stream.flatMap(_.as[String])
        case Failure(_) => _
      }
```

Double slice query:

```scala
  query.slice(Seq("D", "A", "K")).slice(Seq("A", "D", "K")).execute match {
        case Success(rows) => rows.flatten.flatMap(_.as[String])
        case Failure(_) => _
      }
```
