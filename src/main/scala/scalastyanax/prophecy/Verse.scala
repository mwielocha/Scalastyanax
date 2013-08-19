package prophecy

import com.netflix.astyanax.model.{ColumnList, Row, Rows}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 19.08.2013
 * Time: 21:34
 * To change this template use File | Settings | File Templates.
 */
case class Verse[C](columnList: ColumnList[C]) {

  def words: Stream[Word[C]] = columnList.toStream.map(Word(_))

  def interpret[R](implicit interpret: Word[C] => R) = words.map(interpret)

}

object Verse {

  def apply[C](row: Row[_, C]): Verse[C] = {
    Verse[C](row.getColumns)
  }
}
