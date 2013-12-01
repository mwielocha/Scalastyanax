package scalastyanax

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 01.12.2013
 * Time: 14:38
 * To change this template use File | Settings | File Templates.
 */
case class RangeQuery[T](fromOpt: Option[T], toOpt: Option[T], limitOpt: Option[Int], reverse: Boolean = false) {

  def to(to: T): RangeQuery[T] = copy(toOpt = Some(to))

  def from(from: T): RangeQuery[T] = copy(fromOpt = Some(from))

  def take(limit: Int): RangeQuery[T] = copy(limitOpt = Some(limit))

  def reversed: RangeQuery[T] = copy(reverse = true)

  private[scalastyanax] def fromOrNull: T = fromOpt.getOrElse(null.asInstanceOf[T])

  private[scalastyanax] def toOrNull: T = toOpt.getOrElse(null.asInstanceOf[T])

  private[scalastyanax] def limitOrNull: Int = limitOpt.getOrElse(null.asInstanceOf[Int])

}

trait RangeQueryBuilder {

  def from[T](from: T) = RangeQuery[T](Some(from), None, None)

  def to[T](to: T) = RangeQuery[T](None, Some(to), None)

  def take[T](limit: Int) = RangeQuery[T](None, None, Some(limit))

  def reversed[T] = RangeQuery[T](None, None, None, true)

}
