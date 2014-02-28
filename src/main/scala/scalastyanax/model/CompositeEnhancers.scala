package scalastyanax.model

import com.netflix.astyanax.model.Composite
import com.netflix.astyanax.Serializer
import scala.collection.JavaConversions._

/**
 * author mikwie
 *
 */

object CompositeImplicits extends CompositeEnhancers

trait CompositeEnhancers {

  implicit class EnhancedComposite(composite: Composite) {

    def apply(index: Int): Option[AnyRef] = composite.zipWithIndex.find(_._2 == index)

    def apply[S](index: Int, serializer: Serializer[S]): Option[S] = {
      composite.componentsIterator.zipWithIndex.find(_._2 == index).map {
        case (component, index) => component.getValue(serializer)
      }
    }
  }
}
