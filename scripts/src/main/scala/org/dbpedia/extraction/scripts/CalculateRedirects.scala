package org.dbpedia.extraction.scripts

import java.io.File

import org.dbpedia.extraction.util.RichFile.wrapFile
import org.dbpedia.extraction.dump.extract.{Config, ConfigLoader}
import org.dbpedia.extraction.util.{ConfigUtils, Finder}

/**
 * Created by Chile on 2/26/2015.
 */
object CalculateRedirects {
  def main(args: Array[String]) : Unit = {
    require(args != null && args.length == 1, "One arguments required, extraction config file and extension to work with")
    require(args(0).nonEmpty, "missing required argument: config file name")

    val configProps = ConfigUtils.loadConfig(args(0), "UTF-8")
    val config = new Config(configProps)
    val configLoader = new ConfigLoader(config)

    val langConfString = ConfigUtils.getString(configProps, "languages", false)
    val languages = ConfigUtils.parseLanguages(config.dumpDir, Seq(langConfString)).toList.par

    languages.foreach(lang => {
      val finder = new Finder[File](config.dumpDir, lang, config.wikiName)
      configLoader.createDumpExtractionContext(lang, finder)
    })
  }
}
