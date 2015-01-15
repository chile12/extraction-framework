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

  val in = getClass.getResourceAsStream(file)
  try {
    val titles =
      for (line <- Source.fromInputStream(in)(Codec.UTF8).getLines
        if(!line.trim().startsWith("#")))
          yield
          {
            line.toLowerCase().trim()
          }

    rdfTemplate ++= titles.toList
  }
  finally in.close

  rdfTemplate.reverse

  val prefixSeq = rdfTemplate.slice(0,rdfTemplate.lastIndexWhere(_.trim().startsWith("@prefix"))+1)
  var index = rdfTemplate.indexWhere(_.contains("rml:logicalsource"))
  var start = rdfTemplate.lastIndexWhere(_.contains("mapping:{title}"), index)
  var end = rdfTemplate.indexWhere(_.endsWith("."), index) +1
  val sourceSeq = rdfTemplate.slice(start, end)
  index = rdfTemplate.indexWhere(_.contains("{map-to-class}"))
  start = rdfTemplate.lastIndexWhere(_.contains("mapping:{title} "), index)
  end = rdfTemplate.indexWhere(_.endsWith("."), start+1) +1
  val classSeq = rdfTemplate.slice(start, end)
  index = rdfTemplate.indexWhere(_.contains("{template-property}"))
  start = rdfTemplate.lastIndexWhere(_.contains("mapping:{title} "), index)
  end = rdfTemplate.indexWhere(_.endsWith("."), start+1) +1
  val propSeq = rdfTemplate.slice(start, end)

  protected val parser = WikiParser.getInstance()

  def getRdfTemplate(): String =
  {
    val B = new StringBuilder()
    prefixSeq.foreach(x => B.append(x + "\n"))
    B.append("\n")
    prefixSeq.foreach((B.append(_)))
    B.toString()
  }

  def getRdfHttpTemplate() : Elem =
  {
    val mapps = mappings.templateMappings.head._2.asInstanceOf[TemplateMapping].mappings.collect
    {
      case simpleProp : SimplePropertyMapping => simpleProp
    }

    <div>
      <div>
        {getHttpRows(prefixSeq)}
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
      <p>
        {
          in.map(x => replaceParams(x) ++ <br/>)
        }
      </p>
  }

  private def getHttpPropertyRow(templateProperty: String, ontologyProperty: String): Elem =
  {
    <div>
      <p>
        {
          propSeq.map(x => replaceProperty(replaceParams(x), templateProperty.trim().replaceAllLiterally(" ", "%20"), ontologyProperty.trim().replaceAllLiterally(" ", "%20")) ++ <br/>)
        }</p>
    </div>
  }

  private def replaceParams(in: String): String =
  {
    var out = in.replaceAllLiterally("{title}", page.title.encoded.toString())
    out = out.replaceAllLiterally("{map-to-class}", mappings.templateMappings.head._2.asInstanceOf[TemplateMapping].mapToClass.name)
    out.replaceAllLiterally("{lang}", lang.wikiCode)
  }

  private def replaceProperty(in: String, templateProperty: String, ontologyProperty: String): String =
  {
    var out = in.replaceAllLiterally("{template-property}", templateProperty)
    out.replaceAllLiterally("{ontology-property}", ontologyProperty)
  }
}
