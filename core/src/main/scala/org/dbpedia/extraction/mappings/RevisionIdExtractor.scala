package org.dbpedia.extraction.mappings

import org.dbpedia.extraction.destinations.{DBpediaDatasets, Quad, QuadBuilder}
import org.dbpedia.extraction.ontology.Ontology
import org.dbpedia.extraction.sources.WikiPage
import org.dbpedia.extraction.util.Language

import scala.language.reflectiveCalls

/**
 * Extracts revision ids of articles, e.g.
 * <http://dbpedia.org/resource/Foo> <http://dbpedia.org/ontology/wikiPageRevisionID> "123456"^^<xsd:integer> .
 */
class RevisionIdExtractor (
  context: {
    def ontology: Ontology
    def language: Language
  }
)
extends WikiPageExtractor
{
  private val wikiPageRevisionIdProperty = context.ontology.properties("wikiPageRevisionID")

  override val datasets = Set(DBpediaDatasets.RevisionIds)

  private val quad = QuadBuilder(context.language, DBpediaDatasets.RevisionIds, wikiPageRevisionIdProperty, context.ontology.datatypes("xsd:integer")) _

  override def extract(page: WikiPage, subjectUri: String, pageContext: PageContext): Seq[Quad] = {
    //TODO change line number
    Seq(quad(subjectUri, page.revision.toString, page.sourceUri, -1))
  }
}