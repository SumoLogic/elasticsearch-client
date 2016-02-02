package com.sumologic.elasticsearch.restlastic.dsl

trait IndexDsl extends DslCommons {
  case object CreateIndex extends RootObject {
    override def toJson: Map[String, Any] = Map()
  }

  case class Document(id: String, data: Map[String, Any]) extends EsOperation with RootObject {
    override def toJson: Map[String, Any] = data
  }

  // Op types are lowercased to avoid a conflict with the Index case class
  sealed trait OperationType { val jsonStr: String }
  case object index extends  OperationType {
    override val jsonStr: String = "index"
  }
  case object create extends OperationType {
    override val jsonStr: String = "create"
  }
  case object update extends OperationType {
    override val jsonStr: String = "update"
  }

  case class Bulk(operations: Seq[BulkOperation]) extends EsOperation with RootObject {
    override def toJson: Map[String, Any] = throw new UnsupportedOperationException
    override lazy val toJsonStr = operations.map(_.toJsonStr).mkString("", "\n", "\n")
  }

  case class BulkOperation(operation: OperationType, location: Option[(Index, Type)], document: Document) extends EsOperation {
    import EsOperation.compactJson
    override def toJson: Map[String, Any] = throw new UnsupportedOperationException
    def toJsonStr: String = {
      val jsonObjects = Map(operation.jsonStr ->
        (Map("_id" -> document.id) ++ location.map { case (index, tpe) => Map("_index" -> index.name, "_type" -> tpe.name)}.getOrElse(Map()))
      )
      s"${compactJson(jsonObjects)}\n${document.toJsonStr}"
    }
  }
}


