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

  sealed trait IndexOption {
    val option: String
  }

  // Keyword datatype - https://www.elastic.co/guide/en/elasticsearch/reference/5.1/keyword.html
  case object KeywordType extends FieldType {
    val rep = "keyword"
  }

  // Text data type - https://www.elastic.co/guide/en/elasticsearch/reference/5.1/text.html
  case object TextType extends FieldType {
    val rep = "text"
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

  trait ContextType extends FieldType

  case object CategoryContextType extends ContextType {
    val rep = "category"
  }

  case object GeoContextType extends ContextType {
    val rep = "geo"
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

  // Supported in elasticsearch v2.4
  case object DocsIndexOption extends IndexOption {
    val option = "docs"
  }

  // Supported in elasticsearch v2.4
  case object FreqsIndexOption extends IndexOption {
    val option = "freqs"
  }

  // Supported in elasticsearch v2.4
  case object PositionsIndexOption extends IndexOption {
    val option = "positions"
  }

  case object OffsetsIndexOption extends IndexOption {
    val option = "offsets"
  }

  case class Mapping(tpe: Type, mapping: IndexMapping) extends RootObject {
    override def toJson: Map[String, Any] = Map(tpe.name -> mapping.toJson)
  }

  case class IndexMapping(fields: Map[String, FieldMapping],
                          enableAllFieldOpt: Option[Boolean] = None)
    extends EsOperation {
    val _properties = "properties"
    val _all = "_all"
    val _enabled = "enabled"
    override def toJson: Map[String, Any] = {
      Map(_properties -> fields.mapValues(_.toJson)) ++
        enableAllFieldOpt.map(f => _all -> Map(_enabled -> f))
    }
  }

  sealed trait FieldMapping extends EsOperation {
    val _type = "type"
  }

  case class BasicFieldMapping(tpe: FieldType,
                               boostOpt: Option[Float] = None,
                               docValuesOpt: Option[Boolean] = None,
                               storeOpt: Option[Boolean] = None,
                               nullValueOpt: Option[Boolean] = None,
                               analyzerOpt: Option[Name] = None,
                               searchAnalyzerOpt: Option[Name] = None,
                               ignoreAboveOpt: Option[Int] = None,
                               indexOptionsOpt: Option[IndexOption] = None) extends FieldMapping {

    val _boost = "boost"
    val _doc_values = "doc_values"
    val _store = "store"
    val _null_value = "null_value"
    val _analyzer = "analyzer"
    val _search_analyzer = "search_analyzer"
    val _ignore_above = "ignore_above"
    val _index_options = "index_options"

    override def toJson: Map[String, Any] =
      Map(_type -> tpe.rep) ++
        boostOpt.map(_boost -> _) ++
        docValuesOpt.map(_doc_values -> _) ++
        storeOpt.map(_store -> _) ++
        nullValueOpt.map(_null_value -> _) ++
        analyzerOpt.map(_analyzer -> _.name) ++
        searchAnalyzerOpt.map(_search_analyzer -> _.name) ++
        ignoreAboveOpt.map(_ignore_above -> _) ++
        indexOptionsOpt.map(_index_options -> _.option)
  }

  case class BasicObjectMapping(fields: Map[String, FieldMapping]) extends FieldMapping {
    val _properties = "properties"
    override def toJson: Map[String, Any] = Map(_properties -> fields.mapValues(_.toJson))
  }

  trait Completion extends FieldMapping{
    val _analzyer = "analyzer"
    val _search_analyzer = "search_analyzer"
    val _preserve_separators = "preserve_separators"
    val _preserve_position_increments = "preserve_position_increments"
    val _max_input_length = "max_input_length"

    def analyzerOpt: Option[Name]
    def search_analyzerOpt: Option[Name]
    def preserve_separatorsOpt: Option[Boolean]
    def preserve_position_incrementsOpt: Option[Boolean]
    def max_input_lengthOpt: Option[Int]

    def toJson: Map[String, Any] = {
      Map(_type -> "completion") ++
      analyzerOpt.map(_analzyer -> _.name) ++
      search_analyzerOpt.map(_search_analyzer -> _.name) ++
      preserve_separatorsOpt.map(_preserve_separators -> _) ++
      preserve_position_incrementsOpt.map(_preserve_position_increments -> _) ++
      max_input_lengthOpt.map(_max_input_length -> _)
    }
  }

  case class CompletionMapping(context: Map[String, CompletionContext],
                               analyzerOpt: Option[Name] = None,
                               search_analyzerOpt: Option[Name] = None,
                               preserve_separatorsOpt: Option[Boolean] = None,
                               preserve_position_incrementsOpt: Option[Boolean] = None,
                               max_input_lengthOpt: Option[Int] = None)
    extends Completion {

    override def toJson: Map[String, Any] = {
      val _context = "contexts"
      val _name = "name"
      val _type = "type"
      val _path = "path"
      val _precision = "precision"
      super.toJson ++
      Map(_context -> context.map { case (name, cc) =>
          Map(_name -> name, _type -> cc.tpe.rep) ++
          cc.pathOpt.map(_path -> _) ++
          cc.precisionOpt.map(_precision -> _)
        }
      )
    }
  }

  case class CompletionContext(tpe: ContextType, pathOpt: Option[String] = None, precisionOpt: Option[Int] = None)

  case object NestedFieldMapping extends FieldMapping {
    override def toJson: Map[String, Any] = Map(_type -> "nested")
  }
}


