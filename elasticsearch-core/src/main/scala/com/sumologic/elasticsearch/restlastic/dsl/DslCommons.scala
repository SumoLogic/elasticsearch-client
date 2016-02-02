package com.sumologic.elasticsearch.restlastic.dsl

import org.json4s.Extraction._
import org.json4s._
import org.json4s.native.JsonMethods._

trait DslCommons {

  trait EsOperation {
    def toJson: Map[String, Any]
  }

  object EsOperation {
    implicit val formats = org.json4s.DefaultFormats
    def compactJson(map: Map[String, Any]) = compact(render(decompose(map)))
  }

  trait RootObject extends EsOperation {
    import EsOperation._
    def toJsonStr: String = compactJson(toJson)
  }

  case object EmptyObject extends RootObject {
    override def toJson: Map[String, Any] = Map()
  }

  case object NoOp extends RootObject {
    override def toJson: Map[String, Any] = throw new UnsupportedOperationException
    override def toJsonStr = ""
  }

  abstract class SingleField(field: String, value: EsOperation) extends EsOperation {
    override def toJson: Map[String, Any] = Map(
      field -> value.toJson
    )
  }

  case class Index(name: String)

  case class Type(name: String)
}


