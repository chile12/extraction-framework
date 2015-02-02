package org.dbpedia.extraction.mappings

import java.io._
import java.util.logging.{Level, Logger}

import org.dbpedia.extraction.sources.{Source, WikiPage}
import org.dbpedia.extraction.util.{Language, UriUtils}
import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.wikiparser.impl.wikipedia.Redirect

import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet}

/**
 * Holds the redirects between wiki pages
 * At the moment, only redirects between Templates are considered
 *
 * @param map Redirect map. Contains decoded template titles.
 */
// FIXME: this class is a hack. Resolving redirects is a central part of DBpedia and should be done
// right and more explicitly. This class is trying to do too much under the hood.
// FIXME: this class does basically the same thing as RedirectExtractor, just differently.
//TODO make map private?
//TODO language dependent
class Redirects(private val map : Map[String, String])
{
    /**
     * Resolves a redirect.
     *
     * @param title The title of the page
     * @return If this page is a redirect, the destination of the redirect.
     * If this page is not a redirect, the page itself.
     */
    def resolve(title : WikiTitle) : WikiTitle =
    {
        // if there is no redirect for given title, just return same object
        if (! map.contains(title.decodedWithNamespace)) return title
        
        //Remember already visited pages to avoid cycles
        val visited = new HashSet[String]()

        //Follows redirects
        val currentTitle = resolveTitle(title.decodedWithNamespace)

        if(currentTitle == title.decodedWithNamespace)
          title
        else
          new WikiTitle(currentTitle, Namespace.Template, title.language)
    }

    /**
     * resolves a redirect as an uri
     * @param resolveUri
     * @return uri of resolved page
     */
    def resolve(resolveUri : String, lang: Language) : String =
    {
      if(!resolveUri.contains("dbpedia.org/resource/"))  //not!
        return resolveUri

      val titleName = UriUtils.encode(resolveUri).getPath()
      val title = titleName.substring(titleName.lastIndexOf("/")+1).replace('_', ' ')

      val resolvedTitle = resolveTitle(title)
      if(resolvedTitle == title)
        return resolveUri
      else
        return new WikiTitle(resolvedTitle, Namespace.Template, lang).encoded.toString()
    }

    private def resolveTitle(title: String) : String = {
      //Remember already visited pages to avoid cycles
      val visited = new mutable.HashSet[String]()

      //Follows redirects
      var currentTitle = title
      while (!visited.contains(currentTitle)) {
        visited.add(currentTitle)
        map.get(currentTitle) match {
          case Some(destinationTitle) => currentTitle = destinationTitle
          case None => return currentTitle
        }
      }
      //Detected a cycle
      title
    }

    /**
     * TODO: comment. What does this method do?
     */
    def resolveMap[T](mappings : Map[String, T]) : Map[String, T] =
    {
        val resolvedMappings = new HashMap[String, T]()

        for((source, destination) <- map if !mappings.contains(source))
        {
            //Remember already visited pages to avoid cycles
            val visited = new HashSet[String]()
            visited.add(source)

            //Compute transitive hull
            var lastDestination = source
            var currentDestination = destination
            while(currentDestination != null && !visited.contains(currentDestination))
            {
                 visited.add(currentDestination)
                 lastDestination = currentDestination
                 currentDestination = map.get(currentDestination).getOrElse(null)
            }

            //Add to redirect map
            for(destinationT <- mappings.get(lastDestination))
            {
                resolvedMappings(source) = destinationT
            }
        }

        for ((source, destination) <- map if !mappings.contains(destination)) {
          if (mappings.contains(source)) {
            resolvedMappings(destination) = mappings(source)
          }
        }

        return (mappings ++ resolvedMappings)
    }
}

/**
 * Loads redirects from a cache file or source of Wiki pages.
 * At the moment, only redirects between Templates are considered
 */
object Redirects
{
    private val logger = Logger.getLogger(classOf[Redirects].getName)

    /**
     * Tries to load the redirects from a cache file.
     * If not successful, loads the redirects from a source.
     * Updates the cache after loading the redirects from the source.
     */
    def load(source : Source, cache : File, lang : Language) : Redirects =
    {
        //Try to load redirects from the cache
        try
        {
           return loadFromCache(cache)
        }
        catch
        {
            case ex : Exception => logger.log(Level.INFO, "Will extract redirects from source for "+lang.wikiCode+" wiki, could not load cache file '"+cache+"': "+ex)
        }

        //Load redirects from source
        val redirects = loadFromSource(source, lang)
        
        val dir = cache.getParentFile
        if (! dir.exists && ! dir.mkdirs) throw new IOException("cache dir ["+dir+"] does not exist and cannot be created")
        val outputStream = new ObjectOutputStream(new FileOutputStream(cache))
        try
        {
            outputStream.writeObject(redirects.map)
        }
        finally
        {
            outputStream.close()
        }
        logger.info(redirects.map.size + " redirects written to cache file "+cache)

        redirects
    }

    /**
     * Loads the redirects from a cache file.
     */
    private def loadFromCache(cache : File) : Redirects =
    {
        logger.info("Loading redirects from cache file "+cache)
        val inputStream = new ObjectInputStream(new FileInputStream(cache))
        try
        {
            val redirects = new Redirects(inputStream.readObject().asInstanceOf[Map[String, String]])

            logger.info(redirects.map.size + " redirects loaded from cache file "+cache)
            redirects
        }
        finally
        {
            inputStream.close()
        }
    }

    /**
     * Loads the redirects from a source.
     */
    def loadFromSource(source : Source, lang : Language) : Redirects =
    {
        logger.info("Loading redirects from source ("+lang.wikiCode+")")

        val redirectFinder = new RedirectFinder(lang)

        // TODO: usually, flatMap can be applied to Option, but not here. That's why
        // RedirectFinder.apply returns a List, not an Option. Some implicit missing?
        val redirects = new Redirects(source.flatMap(redirectFinder).toMap)

        logger.info("Redirects loaded from source ("+lang.wikiCode+")")
        redirects
    }

    private class RedirectFinder(lang : Language) extends (WikiPage => List[(String, String)])
    {
        val regex = buildRegex
        
        private def buildRegex = {
            val redirects = Redirect(lang).mkString("|")
            // (?ius) enables CASE_INSENSITIVE UNICODE_CASE DOTALL
            // case insensitive and unicode are important - that's what mediawiki does.
            // Note: Although we do not specify a Locale, UNICODE_CASE does mostly the right thing.
            // DOTALL means that '.' also matches line terminators.
            // Reminder: (?:...) are non-capturing groups, '*?' is a reluctant qualifier.
            // (?:#[^\n]*?)? is an optional (the last '?') non-capturing group meaning: there may
            // be a '#' after which everything but line breaks is allowed ('[]{}|<>' are not allowed 
            // before the '#'). The match is reluctant ('*?'), which means that we recognize ']]' 
            // as early as possible.
            // (?:\|[^\n]*?)? is another optional non-capturing group that reluctantly consumes 
            // a '|' character and everything but line breaks after it.
            ("""(?ius)\s*(?:"""+redirects+""")\s*:?\s*\[\[([^\[\]{}|<>\n]+(?:#[^\n]*?)?)(?:\|[^\n]*?)?\]\].*""").r
        } 

        override def apply(page : WikiPage) : List[(String, String)]=
        {
            var destinationTitle : WikiTitle = 
            page.source match {
                case regex(destination) => {
                  try {
                      
                      WikiTitle.parse(destination, page.title.language)
                  }
                  catch {
                      case ex : WikiParserException => {
                          Logger.getLogger(Redirects.getClass.getName).log(Level.WARNING, "Couldn't parse redirect destination", ex)
                          null
                      }
                  }
                }
                case _ => null 
            }
            
            if (destinationTitle != page.redirect) {
                Logger.getLogger(Redirects.getClass.getName).log(Level.WARNING, "wrong redirect. page: ["+page.title+"].\nfound by dbpedia:   ["+destinationTitle+"].\nfound by wikipedia: ["+page.redirect+"]")
            }

          if (destinationTitle != null && page.title.namespace == destinationTitle.namespace
            && (page.title.namespace == Namespace.Template || page.title.namespace == Namespace.Main))
            {
                List((page.title.decodedWithNamespace, destinationTitle.decodedWithNamespace))
            }
            else
            {
                Nil
            }
        }
    }
}