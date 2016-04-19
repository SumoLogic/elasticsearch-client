/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

  case object NotIndexedIndex extends IndexType {
    val rep = "no"
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

  case class IndexMapping(fields: Map[String, FieldMapping], enabled:EnabledFieldMapping) extends EsOperation {
    override def toJson: Map[String, Any] = Map(_properties -> fields.mapValues(_.toJson), _timestamp -> enabled.toJson)
  }

  sealed trait FieldMapping extends EsOperation

  val _properties = "properties"
  val _timestamp = "_timestamp"
  val _type = "type"
  val _index = "index"

  case class BasicFieldMapping(tpe: FieldType, index: Option[IndexType]) extends FieldMapping {
    override def toJson: Map[String, Any] = Map(_type -> tpe.rep) ++ index.map(_index -> _.rep).toList.toMap
  }

  case class BasicObjectMapping(fields: Map[String, FieldMapping]) extends FieldMapping {
    override def toJson: Map[String, Any] = Map(_properties -> fields.mapValues(_.toJson))
  }

  case class CompletionMapping(context: Map[String, CompletionContext], caseSensitive: Boolean = true) extends FieldMapping {
    val _type = "type" -> "completion"
    // simple analyzer does case insensitive autocomplete
    val analyzer = if (caseSensitive) "keyword" else "simple"
    val _analzyer = "analyzer" -> analyzer
    val _sanalyzer = "search_analyzer" -> analyzer
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

  case class EnabledFieldMapping(enabled: Boolean) extends FieldMapping {
    val _enabled = "enabled"
    override def toJson: Map[String, Any] = Map(_enabled -> enabled)
  }

  case class CompletionContext(path: String)
}


