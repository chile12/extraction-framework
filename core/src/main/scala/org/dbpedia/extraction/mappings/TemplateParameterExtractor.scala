package org.dbpedia.extraction.mappings

import org.dbpedia.extraction.destinations.{DBpediaDatasets, Quad}
import org.dbpedia.extraction.wikiparser._
import org.dbpedia.extraction.ontology.Ontology
import org.dbpedia.extraction.util.Language
import scala.collection.mutable.ArrayBuffer
import scala.language.reflectiveCalls

/**
 * Extracts template variables from template pages (see http://en.wikipedia.org/wiki/Help:Template#Handling_parameters)
 */
class TemplateParameterExtractor(
  context: { 
    def ontology: Ontology
    def language : Language 
  } 
) 
extends PageNodeExtractor
{
  private val templateParameterProperty = context.language.propertyUri.append("templateUsesParameter")

  val parameterRegex = """(?s)\{\{\{([^|}{<>]*)[|}<>]""".r
  
  override val datasets = Set(DBpediaDatasets.TemplateParameters)

  override def extract(page : PageNode, subjectUri : String, pageContext : PageContext): Seq[Quad] =
  {
    if (page.title.namespace != Namespace.Template || page.isRedirect) return Seq.empty

    val quads = new ArrayBuffer[Quad]()
    val parameters = new ArrayBuffer[(String, Int)]()
    val linkParameters = new ArrayBuffer[(String, Int)]()

    //try to get parameters inside internal links
    for (linkTemplatePar <- collectInternalLinks(page) )  {
      linkParameters += ((linkTemplatePar.toWikiText, linkTemplatePar.line))
    }

    linkParameters.distinct.foreach( link => {
      parameterRegex findAllIn link._1 foreach (_ match {
          case parameterRegex (param) => parameters += ((param, link._2))
          case _ => // ignore
      })
    })

    for (templatePar <- collectTemplateParameters(page) )  {
      parameters += ((templatePar.parameter, templatePar.line))
    }

    for (parameter <- parameters.distinct if parameter._1.nonEmpty) {
      // TODO: page.sourceUri does not include the line number
      quads += new Quad(context.language, DBpediaDatasets.TemplateParameters, subjectUri, templateParameterProperty, 
          parameter._1, page.sourceUri, context.ontology.datatypes("xsd:string"), parameter._2)
    }
    
    quads
  }


  private def collectTemplateParameters(node : Node) : List[TemplateParameterNode] =
  {
    node match
    {
      case tVar : TemplateParameterNode => List(tVar)
      case _ => node.children.flatMap(collectTemplateParameters)
    }
  }

  //TODO check inside links e.g. [[Image:{{{flag}}}|125px|border]]
  private def collectInternalLinks(node : Node) : List[InternalLinkNode] =
  {
    node match
    {
      case linkNode : InternalLinkNode => List(linkNode)
      case _ => node.children.flatMap(collectInternalLinks)
    }
  }

}