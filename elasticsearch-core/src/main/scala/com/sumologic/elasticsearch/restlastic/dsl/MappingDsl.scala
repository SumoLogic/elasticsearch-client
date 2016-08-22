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

  // String datatype - https://www.elastic.co/guide/en/elasticsearch/reference/current/string.html
  case object StringType extends FieldType {
    val rep = "string"
  }


  // Numeric datatypes - https://www.elastic.co/guide/en/elasticsearch/reference/current/number.html
  case object LongType extends FieldType {
    val rep = "long"
  }

  case object IntegerType extends FieldType {
    val rep = "integer"
  }

  case object ShortType extends FieldType {
    val rep = "short"
  }

  case object ByteType extends FieldType {
    val rep = "byte"
  }

  case object DoubleType extends FieldType {
    val rep = "double"
  }

  case object FloatType extends FieldType {
    val rep = "float"
  }


  // Date datatype - https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
  case object DateType extends FieldType {
    val rep = "date"
  }


  // Boolean datatype - https://www.elastic.co/guide/en/elasticsearch/reference/current/boolean.html
  case object BooleanType extends FieldType {
    val rep = "boolean"
  }


  // Binary datatype - https://www.elastic.co/guide/en/elasticsearch/reference/current/binary.html
  case object BinaryType extends FieldType {
    val rep = "binary"
  }

  // Geo point -  https://www.elastic.co/guide/en/elasticsearch/guide/current/geopoints.html
  case object GeoPointType extends FieldType {
    val rep = "geo_point"
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

  case class IndexMapping(fields: Map[String, FieldMapping],
                          enabled: EnabledFieldMapping,
                          enableAllField: Option[Boolean] = None)
    extends EsOperation {
    val _all = "_all"
    val _enabled = "enabled"
    override def toJson: Map[String, Any] = {
      Map(_properties -> fields.mapValues(_.toJson),
        _timestamp -> enabled.toJson) ++
        enableAllField.map(f => _all -> Map(_enabled -> f))
    }
  }

  sealed trait FieldMapping extends EsOperation

  val _properties = "properties"
  val _timestamp = "_timestamp"
  val _type = "type"
  val _index = "index"
  val _analyzer = "analyzer"
  val _searchAnalyzer = "search_analyzer"
  val _ignoreAbove = "ignore_above"

  case class BasicFieldMapping(tpe: FieldType, index: Option[IndexType], analyzer: Option[Name],
                               ignoreAbove: Option[Int] = None, search_analyzer: Option[Name]= None)
    extends FieldMapping {

    override def toJson: Map[String, Any] = Map(
      _type -> tpe.rep) ++
      index.map(_index -> _.rep) ++
      analyzer.map(_analyzer -> _.name) ++
      search_analyzer.map(_searchAnalyzer -> _.name) ++
      ignoreAbove.map(_ignoreAbove -> _).toList.toMap
  }

  case class BasicObjectMapping(fields: Map[String, FieldMapping]) extends FieldMapping {
    override def toJson: Map[String, Any] = Map(_properties -> fields.mapValues(_.toJson))
  }

  trait Completion {
    val _type = "type" -> "completion"
    val _context = "context"
    val _analzyer = "analyzer" -> analyzer.name
    val _sanalyzer = "search_analyzer" -> analyzer.name

    def analyzer: Name

    def toJson: Map[String, Any] = {
      Map(
        _type,
        _analzyer,
        _sanalyzer)
    }
  }

  case class CompletionMapping(context: Map[String, CompletionContext], analyzer: Name = Name("keyword"))
    extends FieldMapping with Completion {

    override def toJson: Map[String, Any] = {
      super.toJson ++
      Map(_context -> context.mapValues { case cc =>
          Map(
            "type" -> "category",
            "path" -> cc.path
          )
        }
      )
    }
  }

  case class CompletionMappingWithoutPath(context: Map[String, Unit], analyzer: Name = Name("keyword"))
    extends FieldMapping with Completion {

    override def toJson: Map[String, Any] = {
      super.toJson ++
        Map(_context -> context.mapValues { case cc =>
          Map("type" -> "category")
         }
      )
    }
  }

  case class EnabledFieldMapping(enabled: Boolean) extends FieldMapping {
    val _enabled = "enabled"
    override def toJson: Map[String, Any] = Map(_enabled -> enabled)
  }

  case class CompletionContext(path: String)

  case object NestedFieldMapping extends FieldMapping {
    val _nested = "nested"
    override def toJson: Map[String, Any] = Map(_type -> _nested)
  }
}


