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

trait AggregationDsl extends DslCommons with QueryDsl {

  sealed trait Aggregation extends EsOperation

  case class AggregationQuery(query: Query, aggs: Aggregation, timeout: Option[Int] = None)
      extends Query with RootObject {

    val _query = "query"
    val _aggs = "aggs"
    val _size = "size"
    val _timeout = "timeout"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_query -> query.toJson(version),
        _aggs -> aggs.toJson(version),
        _size -> 0) ++
          timeout.map(t => _timeout -> s"${t}ms")
    }
  }

  case class TermsAggregation(field: String,
                              include: Option[String], exclude: Option[String],
                              size: Option[Int], shardSize: Option[Int],
                              hint: Option[String] = None,
                              name: Option[String] = None,
                              aggs: Option[Aggregation] = None,
                              order: Option[SortOrder] = None)
      extends Aggregation {

    val _aggsName = name.getOrElse("aggs_name")
    val _terms = "terms"
    val _field = "field"
    val _include = "include"
    val _exclude = "exclude"
    val _size = "size"
    val _shardSize = "shard_size"
    val _hint = "execution_hint"
    val _order = "order"
    val _term = "_term"
    val _aggs = "aggs"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_aggsName ->
          (Map(_terms ->
              (Map(_field -> field)
                  ++ include.map(_include -> _)
                  ++ exclude.map(_exclude -> _)
                  ++ size.map(_size -> _)
                  ++ shardSize.map(_shardSize -> _)
                  ++ hint.map(_hint -> _)
                  ++ order.map(o => _order -> Map(_term -> o.value))))
              ++ aggs.map(_aggs -> _.toJson(version)))
      )
    }
  }

  case class NestedAggregation(path: String, name: Option[String] = None, aggs: Option[Aggregation] = None)
      extends Aggregation {

    val _aggsName = name.getOrElse("aggs_name")
    val _nested = "nested"
    val _path = "path"
    val _aggs = "aggs"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_aggsName ->
          (Map(_nested ->
              Map(_path -> path))
              ++ aggs.map(_aggs -> _.toJson(version)))
      )
    }
  }

  case class TopHitsAggregation(name: String,
                                size: Option[Int],
                                source: Option[Seq[String]],
                                sort: Option[Map[String, SortOrder]])
      extends Aggregation {

    val _topHits = "top_hits"
    val _size = "size"
    val _source = "_source"
    val _sort = "sort"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(name ->
          Map(_topHits ->
              (Map()
                  ++ size.map(_size -> _)
                  ++ source.map(_source -> _)
                  ++ sort.map(_sort -> _.map { case (field, order) => (field, order.value) })
                  )
          )
      )
    }
  }

  case class SamplerAggregation(sampler: Sampler,
                                aggregation: Aggregation,
                                name: Option[String] = None) extends Aggregation {
    val _aggsName = name.getOrElse("sample")
    val _sampler = "sampler"
    val _aggs = "aggs"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _aggsName -> Map(
        _sampler -> sampler.toJson(version),
        _aggs -> aggregation.toJson(version)
      )
    )
  }

  case class Sampler(size: Int, field: Option[String] = None) extends EsOperation {
    val _shardSize = "shard_size"
    val _field = "field"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_shardSize -> size) ++ field.map(f => _field -> f)
    }
  }

  sealed trait CardinalityQuery extends EsOperation

  case class FieldCardinalityAggregation(field: String) extends CardinalityQuery {
    val _field = "field"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_field -> field)
    }
  }

  sealed trait ScriptAggregation extends CardinalityQuery {
    val _script = "script"
  }

  case class StoredScriptCardinalityAggregation(id: String, params: Option[Map[String, String]] = None) extends ScriptAggregation {
    val _id = "id"
    val _params = "params"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_script -> (
        Map()
          ++ Map(_id -> id)
          ++ params.map(_params -> _)
      ))
    }
  }

  case class SourcedScriptCardinalityAggregation(source: String, lang: Option[String] = None) extends ScriptAggregation {
    val _lang = "lang"
    val _source = "source"

    override def toJson(version: EsVersion): Map[String, Any] = {
      version match {
        case V2 =>
          Map(_script -> source)
        case V6 =>
          Map(_script -> (
            Map()
              ++ lang.map(_lang -> _)
              ++ Map(_source -> source)
            ))
      }
    }
  }

  case class CardinalityAggregation(cardinalityQuery: CardinalityQuery,
                                    name: Option[String] = None,
                                    precisionThreshold: Option[Int] = None)
    extends Aggregation {
    val _aggsName = name.getOrElse("field_count")
    val _cardinality = "cardinality"
    val _precisionThreshold = "precision_threshold"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(
        _aggsName -> Map(_cardinality ->
          (Map()
            ++ cardinalityQuery.toJson(version)
            )
        )
      )
    }
  }
}
