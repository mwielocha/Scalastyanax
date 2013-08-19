package scalastyanax

import _root_.prophecy.Verse
import com.netflix.astyanax.Keyspace
import com.netflix.astyanax.model.ColumnFamily
import scala.collection.JavaConversions._

/**
 * Created with IntelliJ IDEA.
 * User: mwielocha
 * Date: 19.08.2013
 * Time: 21:22
 * To change this template use File | Settings | File Templates.
 */
object Prophet {


  def ask[C, K](rowKey: K)(implicit keyspace: Keyspace, cf: ColumnFamily[K, C]): Verse[C] = {
    Verse(keyspace.prepareQuery(cf).getRow(rowKey).execute().getResult)
  }


}
