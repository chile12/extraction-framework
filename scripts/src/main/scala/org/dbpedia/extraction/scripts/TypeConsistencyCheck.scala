package org.dbpedia.extraction.scripts

import java.io.File

import org.dbpedia.extraction.destinations.formatters.UriPolicy
import org.dbpedia.extraction.destinations.{CompositeDestination, Destination, Quad, WriterDestination}
import org.dbpedia.extraction.ontology.{OntologyClass, OntologyProperty}
import org.dbpedia.extraction.ontology.io.OntologyReader
import org.dbpedia.extraction.sources.XMLSource
import org.dbpedia.extraction.util.ConfigUtils._
import org.dbpedia.extraction.util.RichFile.wrapFile
import org.dbpedia.extraction.util.{IOUtils, Language}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
 * Created by Chile on 1/22/2015.
 */
object TypeConsistencyCheck {

  def main(args: Array[String]) {

    require(args != null && args.length >= 3,
      "need at least five args: " +
        /*0*/ "base dir , " +
        /*1*/ "file format suffix , " +
        /*2*/ "output format" //"trix-triples" ,"trix-quads", "turtle-triples", "turtle-quads" ,"n-triples" ,"n-quads" ,"rdf-json"
    )

    require(args(0).nonEmpty, "no config file name")

    val baseDir = new File(if(args(0).endsWith(("/"))) args(0) else args(0) + "/")
    val properties = "instance-types." + args(1)
    require(properties.nonEmpty, "no input dataset name")
    val ontoFile = new File("C:\\Users\\Chile\\Documents\\GitHub\\extraction-framework\\ontology.xml")
    val ontology = new OntologyReader().read(XMLSource.fromFile(ontoFile, Language.Mappings))


    val propFile = "mappingbased-properties." + args(1)

    var mapp: scala.collection.mutable.Map[String, OntologyClass] = null

    val destinations = new ArrayBuffer[Destination]
    var exceptedFile : File = null
    var exceptedDest : WriterDestination= null
    var conjoinedFile : File = null
    var conjoinedDest : WriterDestination= null
    var disjoinedFile : File = null
    var disjoinedDest : WriterDestination= null
    var noTypeFile : File = null
    var noTypeDest : WriterDestination = null
    var destination : CompositeDestination = null

    val rightQuads = new mutable.HashSet[Quad]()
    val noTypeQuads = new mutable.HashSet[Quad]()
    val conjQuads = new mutable.HashSet[Quad]()
    val disjQuads = new mutable.HashSet[Quad]()

    val relatedClasses = new mutable.HashSet[(OntologyClass, OntologyClass)]
    val disjoinedClasses = new mutable.HashSet[(OntologyClass, OntologyClass)]

    // Use all remaining args as keys or comma or whitespace separated lists of keys
    for (dir <- baseDir.listFiles() if (dir.isDirectory() && dir.name.endsWith("wiki"))) {
      breakable {
        val languages = parseLanguages(baseDir, Array(dir.getName().diff("wiki")))

        require(languages.nonEmpty, "no languages")

        val finder = new DateFinder(baseDir, languages(0))
        finder.find(null, true)

        // use LinkedHashMap to preserve order
        mapp = new scala.collection.mutable.HashMap[String, OntologyClass]()

        destination = new CompositeDestination(destinations.toSeq: _*)
        exceptedFile = finder.find("excepted." + args(1))
        exceptedDest = new WriterDestination(() => IOUtils.writer(exceptedFile), UriPolicy.getFormatter(args(2)))
        destinations += exceptedDest
        conjoinedFile = finder.find("conjoined." + args(1))
        conjoinedDest = new WriterDestination(() => IOUtils.writer(conjoinedFile), UriPolicy.getFormatter(args(2)))
        destinations += conjoinedDest
        disjoinedFile = finder.find("disjoined." + args(1))
        disjoinedDest = new WriterDestination(() => IOUtils.writer(disjoinedFile), UriPolicy.getFormatter(args(2)))
        destinations += disjoinedDest
        noTypeFile = finder.find("noType." + args(1))
        noTypeDest = new WriterDestination(() => IOUtils.writer(noTypeFile), UriPolicy.getFormatter(args(2)))
        destinations += noTypeDest
        destination = new CompositeDestination(destinations.toSeq: _*)

        try {
          QuadReader.readQuads(finder, properties, auto = true) { quad =>
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
          QuadReader.readQuads(finder, propFile, auto = true) { quad =>
            evalueQuad(quad)
          }
          forceWriteQuads()
          destination.close()
        }
        catch {
          case e: Exception =>
            Console.err.println(e.printStackTrace())
            break
        }
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

    def evalueQuad(quad: Quad): Unit =
    {
      breakable {
        if (quad.datatype == null) //object is uri
        {
          val obj = try {
            mapp(quad.value)
          } catch {
            case default => null
          }
          val predOpt = ontology.properties.find(x => x._2.uri == quad.predicate)
          var predicate: OntologyProperty = null
          if (predOpt != null && predOpt != None)
            predicate = predOpt.get._2

          if (predicate != null && predicate.range.uri.trim() == "http://www.w3.org/2002/07/owl#Thing") {
            rightQuads += quad
            break
          }
          else if (obj == null || obj == None) {
            noTypeQuads += quad
            break
          }
          else if (predicate == null) {
            //weired stuff
          }

          if (obj.relatedClasses.contains(predicate.range))
            rightQuads += quad
          else {
            if (isDisjoined(obj, predicate.range.asInstanceOf[OntologyClass], true))
              disjQuads += quad
            else
              conjQuads += quad
          }
        }
        else
          rightQuads += quad

        writeQuads()
      }
    }

    def forceWriteQuads(): Unit = {
      writeQuads(true)
    }

    def writeQuads(forceWrite: Boolean = false): Unit = {
      if (conjQuads.size > 999 || forceWrite) {
        conjoinedDest.write(conjQuads)
        conjQuads.clear()
      }
      if (disjQuads.size > 999 || forceWrite) {
        disjoinedDest.write(disjQuads)
        disjQuads.clear()
      }
      if (rightQuads.size > 999 || forceWrite) {
        exceptedDest.write(rightQuads)
        rightQuads.clear()
      }
      if (noTypeQuads.size > 999 || forceWrite) {
        noTypeDest.write(noTypeQuads)
        noTypeQuads.clear()
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
}
