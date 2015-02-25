package org.dbpedia.extraction.scripts

import java.io.{File, Writer}

import org.dbpedia.extraction.destinations._
import org.dbpedia.extraction.destinations.formatters.Formatter
import org.dbpedia.extraction.destinations.formatters.UriPolicy._
import org.dbpedia.extraction.dump.extract.{Config, ConfigLoader}
import org.dbpedia.extraction.mappings.{CompositeParseExtractor, Redirects}
import org.dbpedia.extraction.util.RichFile.wrapFile
import org.dbpedia.extraction.util.{ConfigUtils, Finder, IOUtils}

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Created by Chile on 2/25/2015.
 */
object ResolveRedirects {

  def main(args: Array[String]) : Unit = {
    require(args != null && args.length == 1, "One arguments required, extraction config file and extension to work with")
    require(args(0).nonEmpty, "missing required argument: config file name")

    val configProps = ConfigUtils.loadConfig(args(0), "UTF-8")
    val config = new Config(configProps)
    val configLoader = new ConfigLoader(config)

    val formats = parseFormats(configProps, "uri-policy", "format")
    val format = formats.keySet.toList(0)
    val suffix = if(format.startsWith(".")) format else "." + format

    val langConfString = ConfigUtils.getString(configProps, "languages", false)
    val languages = ConfigUtils.parseLanguages(config.dumpDir, Seq(langConfString))

    for(lang <- languages){

      val classes = config.extractorClasses(lang)
      val finder = new Finder[File](config.dumpDir, lang, config.wikiName)
      val context = configLoader.createDumpExtractionContext(lang, finder)
      val datasets = CompositeParseExtractor.load(classes, context).datasets
      val destinations = createDestination(datasets, context.redirects, finder, formats)

      destinations.open()

      for(dataset <- datasets){
        QuadReader.readQuads(dataset.name, finder.file(finder.dates().last, dataset.name.replace('_', '-') + suffix)) { quad =>

          destinations.write(Seq(quad.copy(dataset = dataset.name, language = lang.isoCode)))
        }
      }
      destinations.close()
    }
  }

  private def createDestination(datasets: Set[Dataset], redirects: Redirects, finder: Finder[File], formats: scala.collection.Map[String, Formatter]) : Destination = {
    val destination = new ArrayBuffer[Destination]()
    val date = finder.dates().last
    for ((suffix, format) <- formats) {
      val datasetDestinations = new HashMap[String, Destination]()
      for (dataset <- datasets) {
        val file = finder.file(date, dataset.name.replace('_', '-') + "-resolved" +'.'+suffix)
        datasetDestinations(dataset.name) = new RedirectDestination(new WriterDestination(writer(file), format), redirects)
      }
      destination += new DatasetDestination(datasetDestinations)
    }
    new CompositeDestination(destination.toSeq: _*)
  }


  private def writer(file: File): () => Writer = {
    () => IOUtils.writer(file)
  }
}
