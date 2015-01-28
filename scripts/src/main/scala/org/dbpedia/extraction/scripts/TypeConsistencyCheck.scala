package org.dbpedia.extraction.scripts

import java.io.{Writer, File}
import java.net.URL

import org.dbpedia.extraction.destinations.formatters.{Formatter, UriPolicy}
import org.dbpedia.extraction.destinations.formatters.UriPolicy._
import org.dbpedia.extraction.destinations._
import org.dbpedia.extraction.ontology.{Ontology, OntologyClass, OntologyProperty}
import org.dbpedia.extraction.ontology.io.OntologyReader
import org.dbpedia.extraction.sources.{WikiSource, XMLSource}
import org.dbpedia.extraction.util.RichFile.wrapFile
import org.dbpedia.extraction.util.{Finder, ConfigUtils, IOUtils, Language}
import org.dbpedia.extraction.wikiparser.Namespace

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ArrayBuffer}
import scala.util.control.Breaks._

/**
 * Created by Chile on 1/22/2015.
 */
object TypeConsistencyCheck {

  /**
   * different datasets where we store the mapped triples depending on their state
   */
  val correctDataset = new Dataset("mappingbased-properties-correct");
  val disjointDataset = new Dataset("mappingbased-properties-disjoint");
  val untypedDataset = new Dataset("mappingbased-properties-untyped");
  val nonDisjointDataset = new Dataset("mappingbased-properties-non-disjoint");
  
  val datasets = Seq(correctDataset, disjointDataset, untypedDataset, nonDisjointDataset)

  val propertyMap = new scala.collection.mutable.HashMap[String, OntologyProperty]

  def main(args: Array[String]) {


    require(args != null && args.length == 2, "Two arguments required, extraction config file and extension to work with")
    require(args(0).nonEmpty, "missing required argument: config file name")
    require(args(1).nonEmpty, "missing required argument: suffix e.g. .ttl.gz")


    val config = ConfigUtils.loadConfig(args(0), "UTF-8")

    val baseDir = ConfigUtils.getValue(config, "base-dir", true)(new File(_))
    if (!baseDir.exists) throw new IllegalArgumentException("dir " + baseDir + " does not exist")
    val langConfString = ConfigUtils.getString(config, "languages", false)
    val languages = ConfigUtils.parseLanguages(baseDir, Seq(langConfString))

    val formats = parseFormats(config, "uri-policy", "format")

    lazy val ontology = {
      val ontologyFile = ConfigUtils.getValue(config, "ontology", false)(new File(_))
      val ontologySource = if (ontologyFile != null && ontologyFile.isFile) {
        XMLSource.fromFile(ontologyFile, Language.Mappings)
      }
      else {
        val namespaces = Set(Namespace.OntologyClass, Namespace.OntologyProperty)
        val url = new URL(Language.Mappings.apiUri)
        WikiSource.fromNamespaces(namespaces, url, Language.Mappings)
      }

      new OntologyReader().read(ontologySource)
    }

    val suffix = args(1)
    val typesDataset = "instance-types." + suffix
    val mappedTripleDataset = "mappingbased-properties." + suffix

    var mapp: scala.collection.mutable.Map[String, OntologyClass] = null

    val relatedClasses = new mutable.HashSet[(OntologyClass, OntologyClass)]
    val disjoinedClasses = new mutable.HashSet[(OntologyClass, OntologyClass)]

    mapp = new scala.collection.mutable.HashMap[String, OntologyClass]()

    for (lang <- languages) {

      // create destination for this language
      val finder = new Finder[File](baseDir, lang, "wiki")
      val date = finder.dates().last
      val destination = createDestination(finder, date, formats)

      try {
        QuadReader.readQuads(lang.wikiCode+": Reading types from "+typesDataset, finder.file(date, typesDataset)) { quad =>
          val q = quad.copy(language = lang.wikiCode) //set the language of the Quad
          mapProperties(quad)
        }
      }
      catch {
        case e: Exception =>
          Console.err.println(e.printStackTrace())
          break
      }


      try {
        destination.open()
        QuadReader.readQuads(lang.wikiCode+": Reading types from " + mappedTripleDataset, finder.file(date, mappedTripleDataset)) { quad =>

          val correctDataset = checkQuad(quad)
          val q = quad.copy(language = lang.wikiCode, dataset = correctDataset.name) //set the language of the Quad
          destination.write(Seq(q))
        }
        destination.close()
      }
      catch {
        case e: Exception =>
          Console.err.println(e.printStackTrace())
          break
      }
    }



    def mapProperties(quad: Quad): Unit =
    {
      breakable {
        val classOption = ontology.classes.find(x => x._2.uri == quad.value)
        var ontoClass: OntologyClass = null
        if (classOption != null && classOption != None)
          ontoClass = classOption.get._2
        else
          break
        if (!mapp.contains(quad.subject)) //not! {
          mapp(quad.subject) = ontoClass
        else {
          if (ontoClass.relatedClasses.contains(mapp(quad.subject)))
            mapp(quad.subject) = ontoClass
        }
      }
    }

    /**
     * Chacks a Quad and returns the new dataset where it should be written depending on the state
     * @param quad
     * @return
     */
    def checkQuad(quad: Quad): Dataset =
    {
      if (quad.datatype != null) {
        return correctDataset
      }

      //object is uri
      val obj = try {
        mapp(quad.value)
      } catch {
        case _: Throwable => return untypedDataset
      }

      val predicate = getProperty(quad.predicate, ontology)


      if (predicate != null && predicate.range.uri.trim().equals("http://www.w3.org/2002/07/owl#Thing")) {
        return correctDataset
      }
      else if (obj == null || obj == None) {
        return untypedDataset
      }
      else if (predicate == null) {
        //weired stuff
      }

      if (obj.relatedClasses.contains(predicate.range))
        return correctDataset
      else {
        if (isDisjoined(obj, predicate.range.asInstanceOf[OntologyClass], true))
          return disjointDataset
        else
          return nonDisjointDataset
      }

    }

    def isDisjoined(objClass : OntologyClass, rangeClass : OntologyClass, clear: Boolean = true) : Boolean = {

      if (clear)
        relatedClasses.clear()

      if(disjoinedClasses.contains((objClass, rangeClass)))
        return true

      if(objClass.disjointWithClasses.contains(rangeClass)) {
        disjoinedClasses.add(objClass, rangeClass)
        disjoinedClasses.add(rangeClass, objClass)
        return true
      }
      if(rangeClass.disjointWithClasses.contains(objClass)) {
        disjoinedClasses.add(objClass, rangeClass)
        disjoinedClasses.add(rangeClass, objClass)
        return true
      }
      relatedClasses.add(objClass, rangeClass)
      relatedClasses.add(rangeClass, objClass)
      for (objClazz <- objClass.relatedClasses) {
        for(rangeClazz <- rangeClass.relatedClasses) {
          if (!relatedClasses.contains(objClazz, rangeClazz)) { //not!
            if (isDisjoined(objClazz, rangeClazz, false))
              return true
          }
          if (!relatedClasses.contains(rangeClazz, objClazz)) { //not!
            if (isDisjoined(rangeClazz, objClazz, false))
              return true
          }
        }
      }
      return false
    }
  }

  // returns an ontologyProperty from a URI and keeps a local cache
  private def getProperty(uri: String, ontology: Ontology) : OntologyProperty = {

    if (propertyMap.contains(uri)) {
      propertyMap(uri)
    } else {
      val predicateOpt = ontology.properties.find(x => x._2.uri == uri)
      val predicate: OntologyProperty =
        if (predicateOpt != null && predicateOpt != None) { predicateOpt.get._2 }
        else (null)
      propertyMap.put(uri, predicate);
      predicate
    }
  }

  private def createDestination(finder: Finder[File], date: String, formats: scala.collection.Map[String, Formatter]) : Destination = {
    val destination = new ArrayBuffer[Destination]()
    for ((suffix, format) <- formats) {
      val datasetDestinations = new HashMap[String, Destination]()
      for (dataset <- datasets) {
        val file = finder.file(date, dataset.name.replace('_', '-')+'.'+suffix)
        datasetDestinations(dataset.name) = new WriterDestination(writer(file), format)
      }

      destination += new DatasetDestination(datasetDestinations)
    }
    new CompositeDestination(destination.toSeq: _*)
  }

  private def writer(file: File): () => Writer = {
    () => IOUtils.writer(file)
  }
}
