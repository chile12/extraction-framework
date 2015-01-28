package org.dbpedia.extraction.destinations

import org.dbpedia.extraction.ontology.datatypes.Datatype
import org.dbpedia.extraction.ontology.OntologyProperty
import org.dbpedia.extraction.util.Language

/**
 * Convenience methods that help to unclutter code. 
 */
object QuadBuilder {

  def apply(language: Language, dataset: Dataset, predicate: OntologyProperty, datatype: Datatype) (subject: String, value: String, context: String, line: Int) =
    new Quad(language, dataset, subject, predicate, value, context, datatype, line)
  
  def dynamicType(language: Language, dataset: Dataset, predicate: OntologyProperty) (subject: String, value: String, context: String, datatype: Datatype, line: Int) =
    new Quad(language, dataset, subject, predicate, value, context, datatype, line)
  
  def stringPredicate(language: Language, dataset: Dataset, predicate: String) (subject: String, value: String, context: String, datatype: Datatype, line: Int) =
    new Quad(language, dataset, subject, predicate, value, context, datatype, line)
  
  def stringPredicate(language: Language, dataset: Dataset, predicate: String, datatype: Datatype) (subject: String, value: String, context: String, line: Int) =
    new Quad(language, dataset, subject, predicate, value, context, datatype, line)
  
  def dynamicPredicate(language: Language, dataset: Dataset) (subject: String, predicate: String, value: String, context: String, datatype: Datatype, line: Int) =
    new Quad(language, dataset, subject, predicate, value, context, datatype, line)
  
  def dynamicPredicate(language: Language, dataset: Dataset, datatype: Datatype) (subject: String, predicate: OntologyProperty, value: String, context: String, line: Int) =
    new Quad(language, dataset, subject, predicate, value, context, datatype, line)
}
