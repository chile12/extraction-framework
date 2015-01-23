package org.dbpedia.extraction.scripts

import org.dbpedia.extraction.dump.extract.{Config, ConfigLoader}
import org.dbpedia.extraction.mappings.{MappingsLoader, TemplateMapping}
import org.dbpedia.extraction.ontology.{OntologyClass, OntologyType}
import org.dbpedia.extraction.util.ConfigUtils._
import org.dbpedia.extraction.util.RichFile.wrapFile
import org.dbpedia.extraction.util.{ConfigUtils, Language}

/**
 * Created by Chile on 1/22/2015.
 */
object TypeMappingTest {

  def main(args: Array[String]) {

    require(args != null && args.length >= 2,
      "need at least five args: " +
        /*0*/ "config file , " +
        /*1*/ "languages or article count ranges (e.g. 'en,fr' or '10000-')")

    require(args(0).nonEmpty, "no config file name")
    val config = new Config(ConfigUtils.loadConfig(args(0), "UTF-8"))
    val langMap = new scala.collection.mutable.HashMap[Language, scala.collection.mutable.Map[String, OntologyType]]()

    val baseDir = config.dumpDir
    val input = "instance-types." + config.formats.head._1
    require(input.nonEmpty, "no input dataset name")

    // Suffix of DBpedia files, for example ".nt", ".ttl.gz", ".nt.bz2" and so on.
    // This script works with .nt or .ttl files, using IRIs or URIs.
    // Does NOT work with .nq or .tql files. (Preserving the context wouldn't make sense.)

    // Use all remaining args as keys or comma or whitespace separated lists of keys
    val languages = parseLanguages(baseDir, args.drop(1))

    require(languages.nonEmpty, "no languages")

    for (language <- languages) {

      val context = new ConfigLoader(config).getContext(language)
      val mappingsLoader = MappingsLoader.load(context)
      val finder = new DateFinder(baseDir, language)

      // use LinkedHashMap to preserve order
      val mapp : scala.collection.mutable.Map[String, OntologyType] = new scala.collection.mutable.HashMap[String, OntologyType]()

      QuadReader.readQuads(finder, input, auto = true) { quad =>
        if (!mapp.contains(quad.value)) //not!
        {
          val tempMap = mappingsLoader.templateMappings.find(x => x._2.isInstanceOf[TemplateMapping] && x._2.asInstanceOf[TemplateMapping].mapToClass.uri == quad.value)
           if(tempMap != None)
             mapp(quad.subject) = tempMap.get._2.asInstanceOf[TemplateMapping].mapToClass
        }
        else
        {
          val tempMap = mappingsLoader.templateMappings.find(x => x._2.isInstanceOf[TemplateMapping] && x._2.asInstanceOf[TemplateMapping].mapToClass.uri == quad.value)
          if(tempMap != None)
          {
            val newClass : OntologyClass = tempMap.get._2.asInstanceOf[TemplateMapping].mapToClass
            if(newClass.relatedClasses.contains(mapp(quad.value)))
              mapp(quad.subject) = newClass
          }
        }
      }
      langMap(language) = mapp
    }
  }
}
