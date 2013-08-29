package scalastyanax

import com.netflix.astyanax.{model => astxm}
import scala.collection.JavaConversions._

case class Rows[K, C](astx: astxm.Rows[K, C]) {

  def apply(key: K): Row[K, C] = Row(astx.getRow(key))

  def keys: Seq[K] = astx.getKeys.toSeq

  def flatten: Stream[Column[C]] = astx.iterator().toStream.flatMap(row => Columns(row.getColumns).stream)

  def toMap = keys.map(key => (key -> this(key))).toMap

}

case class Row[K, C](astx: astxm.Row[K, C]) {

  def columns: Columns[C] = Columns(astx.getColumns)

  def stream: Stream[Column[C]] = columns.stream

  def key: K = astx.getKey

}

case class Columns[C](astx: astxm.ColumnList[C]) {

  def stream: Stream[Column[C]] = astx.toStream.map(Column(_))

  def apply(name: C): Column[C] = Column(astx.getColumnByName(name))
}

case class Column[C](astx: astxm.Column[C]) {

  private def null2Option[T](input: T): Option[T] = {
    input match {
      case null => None
      case notNull => Some(notNull)
    }
  }

  def name: C = astx.getName

  /**
   * Use value[T]
   * @param m
   * @tparam T
   * @return
   */

  @Deprecated
  def as[T](implicit m: Manifest[T]): Option[T] = value[T]

  def value[T](implicit m: Manifest[T]): Option[T] = {
    m.erasure match {
      case clazz if(clazz == classOf[String]) => null2Option(astx.getStringValue.asInstanceOf[T])
      case clazz if(clazz == classOf[Long]) => null2Option(astx.getLongValue.asInstanceOf[T])
      case clazz if(clazz == classOf[Boolean]) => null2Option(astx.getBooleanValue.asInstanceOf[T])
      case clazz if(clazz == classOf[Int]) => null2Option(astx.getIntegerValue.asInstanceOf[T])
    }
  }
}