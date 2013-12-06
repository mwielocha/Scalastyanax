package scalastyanax

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 01.12.2013
 * Time: 14:38
 * To change this template use File | Settings | File Templates.
 */
case class RangeQuery[T](fromOpt: Option[T], toOpt: Option[T], limitOpt: Option[Int], reverse: Boolean = false) {

  def to(column: Option[T]): RangeQuery[T] = copy(toOpt = column)

  def to(column: T): RangeQuery[T] = to(Some(column))

  def from(column: T): RangeQuery[T] = from(Some(column))

  def from(column: Option[T]): RangeQuery[T] = copy(fromOpt = column)

  def take(limit: Int): RangeQuery[T] = copy(limitOpt = Some(limit))

  def reversed: RangeQuery[T] = copy(reverse = true)

  private[scalastyanax] def fromOrNull: T = fromOpt.getOrElse(null.asInstanceOf[T])

  private[scalastyanax] def toOrNull: T = toOpt.getOrElse(null.asInstanceOf[T])

  private[scalastyanax] def limitOrNull: Int = limitOpt.getOrElse(null.asInstanceOf[Int])

}

trait RangeQueryBuilder {

  def from[T](column: Option[T]): RangeQuery[T] = RangeQuery[T](column, None, None)

  def from[T](column: T): RangeQuery[T] = from(Some(column))

  def to[T](column: Option[T]): RangeQuery[T] = RangeQuery[T](None, column, None)

  def to[T](column: T): RangeQuery[T] = to(Some(column))

  def take[T](limit: Int): RangeQuery[T] = RangeQuery[T](None, None, Some(limit))

  def reversed[T]: RangeQuery[T] = RangeQuery[T](None, None, None, true)

}
