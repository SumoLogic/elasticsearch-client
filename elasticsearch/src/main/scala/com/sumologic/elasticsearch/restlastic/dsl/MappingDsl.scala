package com.sumologic.elasticsearch.restlastic.dsl

trait MappingDsl extends DslCommons {

  sealed trait FieldType {
    val rep: String
  }

  case object StringType extends FieldType {
    val rep = "string"
  }

  case object LongType extends FieldType {
    val rep = "long"
  }

  case object ShortType extends FieldType {
    val rep = "short"
  }

  sealed trait IndexType {
    val rep: String
  }

  case object NotAnalyzedIndex extends IndexType {
    val rep = "not_analyzed"
  }

  case object MappingPath {
    val sep = "."
    def createPath(parent: String, child: String) = {
      parent + sep + child
    }
  }

  case class Mapping(tpe: Type, mapping: IndexMapping) extends RootObject {
    override def toJson: Map[String, Any] = Map(tpe.name -> mapping.toJson)
  }

  case class IndexMapping(fields: Map[String, FieldMapping]) extends EsOperation {
    override def toJson: Map[String, Any] = Map(_properties -> fields.mapValues(_.toJson))
  }

  sealed trait FieldMapping extends EsOperation

  val _properties = "properties"
  val _type = "type"
  val _index = "index"

  case class BasicFieldMapping(tpe: FieldType, index: Option[IndexType]) extends FieldMapping {
    override def toJson: Map[String, Any] = Map(_type -> tpe.rep) ++ index.map(_index -> _.rep).toList.toMap
  }

  case class BasicObjectMapping(fields: Map[String, FieldMapping]) extends FieldMapping {
    override def toJson: Map[String, Any] = Map(_properties -> fields.mapValues(_.toJson))
  }

  case class CompletionMapping(context: Map[String, CompletionContext]) extends FieldMapping {
    val _type = "type" -> "completion"
    val _analzyer = "analyzer" -> "keyword"
    val _sanalyzer = "search_analyzer" -> "keyword"
    val _context = "context"

    override def toJson: Map[String, Any] = {
      Map(
        _type,
        _analzyer,
        _sanalyzer,
        _context -> context.mapValues { case cc =>
          Map(
            "type" -> "category",
            "path" -> cc.path
          )
        }
      )
    }
  }

  case class CompletionContext(path: String)
}


