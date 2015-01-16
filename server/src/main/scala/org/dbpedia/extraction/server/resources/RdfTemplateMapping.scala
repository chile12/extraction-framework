package org.dbpedia.extraction.server.resources

import org.dbpedia.extraction.mappings.{SimplePropertyMapping, TemplateMapping}
import org.dbpedia.extraction.util.Language
import org.dbpedia.extraction.wikiparser.{PageNode, WikiParser}

import scala.collection.mutable
import scala.io.{Codec, Source}
import scala.xml.Elem

/**
 * Created by Chile on 1/14/2015.
 */
class RdfTemplateMapping(page: PageNode, lang: Language, mappings: org.dbpedia.extraction.mappings.Mappings)
{
  private val file = "/rdfTemplateTemplate.txt"
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

  def getRdfHttpTemplate() : Elem =
  {
    if(mappings.templateMappings.size < 1)
      throw new Exception("No mappings found for " + page.title.decoded) //TODO??

    val mapps = mappings.templateMappings.head._2.asInstanceOf[TemplateMapping].mappings.collect
    {
      case simpleProp : SimplePropertyMapping => simpleProp
    }

    <div>
      <div>
        {
          getHttpRows(prefixSeq)
        }
      </div>
      <div>
        {getHttpRows(classSeq)}
      </div>
      <div>
        {
          mapps.map(x =>
            getHttpPropertyRow(x.asInstanceOf[SimplePropertyMapping].templateProperty,x.asInstanceOf[SimplePropertyMapping].ontologyProperty.name )) ++ <br/>
        }
      </div>
    </div>
  }

  private def getHttpRows(in: mutable.MutableList[String]): Elem =
  {
    indent =0
      <p>
        {
          in.map(x => replaceParams(x) ++ <br/>)
        }
      </p>
  }

  private def getHttpPropertyRow(templateProperty: String, ontologyProperty: String): Elem =
  {
    indent =0
    <div>
      <p>
        {
          propSeq.map(x => replaceParams(replaceSimpleProperty(x, templateProperty.trim().replaceAllLiterally(" ", "%20"), ontologyProperty.trim().replaceAllLiterally(" ", "%20"))) ++ <br/>)
        }</p>
    </div>
  }

  private def replaceParams(in: String): String =
  {
    var out = in.replaceAllLiterally("{TITLE}", page.title.encoded.toString())
    out = out.replaceAllLiterally("{PAGE-URI}", page.sourceUri)
    out = out.replaceAllLiterally("{MAP-TO-CLASS}", mappings.templateMappings.head._2.asInstanceOf[TemplateMapping].mapToClass.name)
    out.replaceAllLiterally("{LANG}", lang.wikiCode)
//    out = "".padTo(indent, ' ') + out
//    if((out.endsWith(";") || out.endsWith("}")) && indent < 4)
//    indent = 4
//    if(out.endsWith(",") && indent < 8)
//      indent = 8
//    if(out.endsWith(".") || out.endsWith("]"))
//      indent = 0
  }

  private def replaceSimpleProperty(in: String, templateProperty: String, ontologyProperty: String): String =
  {
    var out = in.replaceAllLiterally("\"{TEMPLATE-PROPERTY}\"", "\"" + templateProperty.replaceAllLiterally("%20", " ") + "\"")
    out = out.replaceAllLiterally("{TEMPLATE-PROPERTY}", templateProperty)
    out.replaceAllLiterally("{ONTOLOGY-PROPERTY}", ontologyProperty)
  }
}
