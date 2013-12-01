package scalastyanax

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 22.08.2013
 * Time: 21:50
 * To change this template use File | Settings | File Templates.
 */
case class Range[C](first: Option[C], last: Option[C] = None, limit: Int = 1, reverse: Boolean = false) {

  def from(from: C): Range[C] = copy(first = Some(from))

  def to(to: C): Range[C] = copy(last = Some(to))

  def take(size: Int): Range[C] = copy(limit = size)

  def reversed: Range[C] = copy(reverse = true)

}

object Range {

  def apply[C](): Range[C] = Range[C](None)

  def apply[C](first: C): Range[C] = {
    Range[C](Some(first))
  }

  def apply[C](first: C, last: C): Range[C] = {
    Range[C](Some(first), Some(last))
  }

  def apply[C](first: C, last: C, limit: Int): Range[C] = {
    Range[C](Some(first), Some(last), limit)
  }

  def apply[C](first: C, limit: Int): Range[C] = {
    Range[C](Some(first), None, limit)
  }

  def from[C](from: C): Range[C] = {
    Range(from)
  }

  def from[C](from: C, limit: Int): Range[C] = {
    Range(from, limit)
  }

  def limit[C](limit: Int): Range[C] = {
    Range[C](None, None, limit)
  }
}
