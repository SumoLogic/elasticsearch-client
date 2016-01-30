package com.sumologic.elasticsearch.restlastic.dsl

trait CompletionDsl extends DslCommons with QueryDsl {

  case class Suggest(text: String, completion: Completion) extends Query with RootObject {
    val _suggest = "suggest"
    val _text = "text"
    val _completion = "completion"

    override def toJson: Map[String, Any] = {
      Map(_suggest -> Map(
        _text -> text,
        _completion -> completion.toJson
      ))
    }
  }

  case class Completion(field: String, size: Int, context: Map[String, String]) extends Query {
    val _field = "field"
    val _context = "context"
    val _size = "size"

    def withAdditionalContext(newContext: (String, String)*) = {
      this.copy(context = context ++ newContext)
    }

    override def toJson: Map[String, Any] = Map(
      _field -> field,
      _context -> context,
      _size -> size
    )
  }
}


