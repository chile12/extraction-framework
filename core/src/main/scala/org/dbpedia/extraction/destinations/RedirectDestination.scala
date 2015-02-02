package org.dbpedia.extraction.destinations

import org.dbpedia.extraction.mappings.Redirects
import org.dbpedia.extraction.util.Language
import org.dbpedia.extraction.wikiparser.{Namespace, WikiTitle}

/**
 * Created by Chile on 1/29/2015.
 */
class RedirectDestination (destination: Destination, redirects: Redirects)
  extends WrapperDestination(destination){

  /**
   * Writes quads to this destination. Implementing classes should make sure that this method
   * can safely be executed concurrently by multiple threads.
   */
  override def write(graph: Traversable[Quad]): Unit =
  {
    val graphNew = graph.map( quad => {
      if (quad.datatype == null) {
        val resolved = redirects.resolve(quad.value, Language.map(quad.language))
        if (resolved != quad.value) {
          val title = new WikiTitle(resolved, Namespace.Main, Language.map(quad.language))
          quad.copy(value = title.resourceIri)
        }
        else
          quad
      }
        else
          quad
      }
    )
    super.write(graphNew)
  }
}
