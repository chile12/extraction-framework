package org.dbpedia.extraction.server.resources

import org.dbpedia.extraction.mappings.{SimplePropertyMapping, TemplateMapping}
import org.dbpedia.extraction.util.Language
import org.dbpedia.extraction.wikiparser.{PageNode, WikiParser}

import scala.collection.mutable
import scala.io.{Codec, Source}

/**
 * Created by Chile on 1/14/2015.
 */
class RdfTemplateMapping(page: PageNode, lang: Language, mappings: org.dbpedia.extraction.mappings.Mappings)
{
  private val file = "/rdfTemplateTemplate.txt"
  private val builder = new StringBuilder()
  private val rdfTemplate = new mutable.MutableList[String]
  private var indent = 0

  val in = getClass.getResourceAsStream(file)
  try {
    val titles =
      for (line <- Source.fromInputStream(in)(Codec.UTF8).getLines
        if(line.trim().startsWith("##") || !line.trim().startsWith("#")))
          yield
          {
              line.trim()
          }

    rdfTemplate ++= titles.toList
  }
  finally in.close

  rdfTemplate.reverse

  val prefixSeq = rdfTemplate.slice(0,rdfTemplate.lastIndexWhere(_.trim().startsWith("## end of init statement")))
  var start = rdfTemplate.indexWhere((_.startsWith("## class statement"))) +1
  var end = rdfTemplate.indexWhere(_.endsWith("class statement"), start)
  val classSeq = rdfTemplate.slice(start, end)
  start = rdfTemplate.indexWhere((_.startsWith("## simple predicateObject mapping"))) +1
  end = rdfTemplate.indexWhere(_.endsWith("simple predicateObject mapping"), start)
  val propSeq = rdfTemplate.slice(start, end)

  protected val parser = WikiParser.getInstance()

  def getRdfTemplate() : String =
  {
    if(mappings.templateMappings.size < 1)
      throw new Exception("No mappings found for " + page.title.decoded) //TODO??

    builder.clear()

    var mapps :List[SimplePropertyMapping] = null
    if(mappings.templateMappings.head._2.isInstanceOf[TemplateMapping])
    {
      mapps = mappings.templateMappings.head._2.asInstanceOf[TemplateMapping].mappings.collect {
        case simpleProp: SimplePropertyMapping => simpleProp
      }
    }
    else
      throw new Exception("At this stage only 'simple' TemplateMappings are supported")

    getHttpRows(prefixSeq)
    getHttpRows(classSeq)
    if(mapps != null)
      mapps.map(x =>
        getHttpPropertyRow(x.asInstanceOf[SimplePropertyMapping].templateProperty,x.asInstanceOf[SimplePropertyMapping].ontologyProperty.name))

    builder.toString()
  }

  private def getHttpRows(in: mutable.MutableList[String]): Unit =
  {
    indent =0
    in.map(x => builder.append(replaceParams(x) + "\n"))
  }

  private def getHttpPropertyRow(templateProperty: String, ontologyProperty: String): Unit =
  {
    indent =0
    propSeq.map(x => builder.append(replaceParams(replaceSimpleProperty(x, templateProperty.trim().replaceAllLiterally(" ", "%20"), ontologyProperty.trim().replaceAllLiterally(" ", "%20"))) + "\n"))

  }

  private def replaceParams(in: String): String =
  {
    var out = in.replaceAllLiterally("{TITLE}", page.title.encoded.toString())
    out = out.replaceAllLiterally("{PAGE-URI}", page.sourceUri)
    out = out.replaceAllLiterally("{MAP-TO-CLASS}", mappings.templateMappings.head._2.asInstanceOf[TemplateMapping].mapToClass.name)
    out = out.replaceAllLiterally("{LANG}", lang.wikiCode)
    out = "".padTo(indent, ' ') + out
    if((out.endsWith(";") || out.endsWith("}")) && indent < 4)
    indent = 4
    if(out.endsWith(",") && indent < 8)
      indent = 8
    if(out.endsWith(".") || out.endsWith("]"))
      indent = 0
    return out
  }

  private def replaceSimpleProperty(in: String, templateProperty: String, ontologyProperty: String): String =
  {
    var out = in.replaceAllLiterally("\"{TEMPLATE-PROPERTY}\"", "\"" + templateProperty.replaceAllLiterally("%20", " ") + "\"")
    out = out.replaceAllLiterally("{TEMPLATE-PROPERTY}", templateProperty)
    out.replaceAllLiterally("{ONTOLOGY-PROPERTY}", ontologyProperty)
  }
}
