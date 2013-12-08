Scalastyanax 2.0
============

Astyanax scala wrapper.

Usage:

```scala

import scalastyanax.Scalastyanax._

implicit val keyspace = cluster.getKeyspace(keyspaceName)

lazy val columnFamily = new ColumnFamily[String, String](
    columnFamilyName, // Column Family Name
    StringSerializer.get(), // Key Serializer
    StringSerializer.get(), // Column Serializer
    StringSerializer.get()) // Value Serializer

//create column family (if doesn't exist) with properties

columnFamily.create(
    "default_validation_class" -> "UTF8Type",
    "key_validation_class"     -> "UTF8Type",
    "comparator_type"          -> "UTF8Type"
)

//truncate

columnFamily.truncate

//simple row query:

val values = columnFamily("Row").get match {
    case Success(result) => result.getResult.values[String]
    case Failure(t) => Nil
}

//even simpler one value query:

val queryResult: Option[String] = columnFamily("RowQueryTestKey" -> "RowQueryTestColumn").get match {
    case Success(result) => result.getResult.value[String]
    case Failure(t) => None
}

//one value query with on-the-fly mapping:

val queryResult: Option[Int] = columnFamily("RowQueryTestKey" -> "RowQueryTestColumn").get match {
    case Success(result) => result.getResult.map[String, Int](_.toInt)
    case Failure(t) => None
}

//another on-the-fly value mapping

val values: Iterable[Int] = columnFamily("Row").get match {
    case Success(result) => result.getResult.flatMapValues[String, Int](_.toInt)
    case Failure(t) => Nil
}

//column slice:

val values: Iterable[String] = columnFamily("Row 1", Seq("Column 1", "Column 4")).get match {
    case Success(result) => result.getResult.values[String]
    case Failure(t) => Nil
}

// column slice using a path (tuple) notation:

val values: Iterable[String] = columnFamily("Row 1" -> Seq("Column 1", "Column 4")).get match {
    case Success(result) => result.getResult.values[String]
    case Failure(t) => Nil
}

//row slice / column slice (path notation):

val values: Map[String, Iterable[String]] = columnFamily(Seq("Row 1", "Row 2") -> Seq("Column 1", "Column 2", "Column 3", "Column 4")).get match {
    case Success(result) => result.getResult.toValueMap[String]
    case Failure(t) => Map.empty
}


//column range (path notation):

val values: Iterable[String] = columnFamily("Row" -> (from("Column 2") to("Column 8") take 4)).get match {
    case Success(result) => result.getResult.values[String]
    case Failure(t) => Nil
}

//simple column mutation:

(columnFamily += ("RowQueryTestKey" -> "RowQueryTestColumn" -> "RowQueryTestValue")).execute

//batch mutation:

val batch = keyspace.newMutationBatch { implicit batch =>
    columnFamily ++= ("Row 1" -> "Column 1" -> "Value 1")
    columnFamily ++= ("Row 1" -> "Column 2" -> "Value 2")
    columnFamily ++= ("Row 1" -> "Column 3" -> "Value 3")
    columnFamily ++= ("Row 1" -> "Column 4" -> "Value 4")
}.execute

//re-using a batch:

val batch = keyspace.newMutationBatch { implicit batch =>
    columnFamily ++= ("Row 1" -> "Column 1" -> "Value 1")
    columnFamily ++= ("Row 1" -> "Column 2" -> "Value 2")
    columnFamily ++= ("Row 1" -> "Column 3" -> "Value 3")
    columnFamily ++= ("Row 1" -> "Column 4" -> "Value 4")
}

keyspace.withBatchMutation(batch) { implicit batch =>
  columnFamily ++= ("A new row in an old batch" -> "Column 1" -> "Value 1")
}.execute

```

Counter columns:

```

val columnFamilyWithCounterColumns = new ColumnFamily[String, String](
    columnFamilyWithCounterColumnsName, // Column Family Name
    StringSerializer.get(), // Key Serializer
    StringSerializer.get(), // Column Serializer
    LongSerializer.get()) // Value Serializer

columnFamilyWithCounterColumns.create(
    "default_validation_class" -> "CounterColumnType",
    "key_validation_class"     -> "UTF8Type",
    "comparator_type"          -> "UTF8Type"
)

//increment:

(columnFamilyWithCounterColumns ^= ("RowWithCounterColumn #1" -> "CounterColumn #1" -> 6)).execute

//increment in a batch:

keyspace.newMutationBatch { implicit batch =>
    columnFamilyWithCounterColumns ^^= ("RowWithCounterColumn #2" -> "CounterColumn #2" -> 100)
}.execute

```

Legacy 1.0 API
============

First define an implicit query context:

```scala
implicit val queryContext = QueryContext(keyspace, columnFamily)
      .withConsistencyLevel(ConsistencyLevel.CL_ONE)
      .withRetryPolicy(RunOnce.get())
```

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
