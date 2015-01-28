package org.dbpedia.extraction.destinations.formatters

/**
 * Helps to render one triple/quad.
 * 
 * Objects of this class are not re-usable - create a new object for each triple.
 */
trait TripleBuilder {
  
  def start(context: String): Unit
  
  def subjectUri(context: String): Unit
  
  def predicateUri(context: String): Unit
  
  def objectUri(context: String): Unit
  
  def plainLiteral(value: String, isoLang: String): Unit
  
  def typedLiteral(value: String, datatype: String): Unit

  def lineNo(line: Int): Unit

  def end(context: String): Unit

  def newLine(): Unit
  
  def result(): String
}