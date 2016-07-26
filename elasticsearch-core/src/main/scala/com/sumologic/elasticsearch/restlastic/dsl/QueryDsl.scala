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

trait QueryDsl extends DslCommons {

  trait Query extends EsOperation

  sealed trait BoolQuery extends EsOperation

  sealed trait Filter extends EsOperation

  case class QueryRoot(query: Query,
                       fromOpt: Option[Int] = None,
                       sizeOpt: Option[Int] = None,
                       sortOpt: Option[Seq[(String, String)]] = None)
    extends RootObject {

    val _query = "query"
    val _size = "size"
    val _sort = "sort"
    val _order = "order"
    val _from = "from"

    override def toJson: Map[String, Any] = {
      Map(_query -> query.toJson) ++
        fromOpt.map(_from -> _) ++
        sizeOpt.map(_size -> _) ++
        sortOpt.map(_sort -> _.map {
          case (field, order) => field -> Map(_order -> order) })
    }
  }

  case class ConstantScore(filter: Filter) extends SingleField("constant_score", filter) with Filter


  case class FilteredQuery(filter: Filter, query: Query) extends Query {
    val _filtered = "filtered"
    val _filter = "filter"
    val _query = "query"
    val _searchType = "search-type"

    override def toJson: Map[String, Any] = {
      Map(
        _filtered -> Map(
          _query -> query.toJson,
          _filter -> filter.toJson
        )
      )
    }
  }

  case class MultiTermFilteredQuery(query: Query, filter: Filter*) extends Query {
    val _filtered = "filtered"
    val _filter = "filter"
    val _query = "query"
    val _searchType = "search-type"
    val _bool = "bool"
    val _must = "must"

    override def toJson: Map[String, Any] = {
      Map(
        _filtered -> Map(
          _query -> query.toJson,
          _filter -> Map(_bool -> Map(_must -> filter.map(_.toJson)))
        )
      )
    }
  }

  case class TermFilter(term: String, value: String) extends Filter {
    val _term = "term"

    override def toJson: Map[String, Any] = {
      Map(_term -> Map(term -> value))
    }
  }

  case class PrefixFilter(field: String, prefix: String) extends Filter {
    val _prefix = "prefix"

    override def toJson: Map[String, Any] = {
      Map(_prefix -> Map(field -> prefix))
    }
  }

  case class Bool(queries: BoolQuery*) extends Query {
    val _bool = "bool"
    val queryMap = queries.map(_.toJson).map(map => (map.keys.head, map(map.keys.head))).toMap

    override def toJson: Map[String, Any] = Map(_bool -> queryMap)
  }

  case class Should(opts: Query*) extends BoolQuery {
    val _should = "should"

    override def toJson: Map[String, Any] = {
      Map(_should -> opts.map(_.toJson))
    }
  }

  case class Must(opts: Query*) extends BoolQuery {
    val _must = "must"

    override def toJson: Map[String, Any] = {
      Map(_must -> opts.map(_.toJson))
    }
  }

  case class MustNot(opts: Query*) extends BoolQuery {
    val _mustnot = "must_not"

    override def toJson: Map[String, Any] = {
      Map(_mustnot -> opts.map(_.toJson))
    }
  }

  case class RangeQuery(key: String, bounds: RangeBound*) extends Query {
    val _range = "range"
    val boundsMap = Map(key -> (bounds :\ Map[String, Any]())(_.toJson ++ _))

    override def toJson: Map[String, Any] =  Map(_range -> boundsMap)
  }

  sealed trait RangeBound extends EsOperation

  case class Gt(value: String) extends RangeBound {
    val _gt = "gt"

    override def toJson: Map[String, Any] = Map(_gt -> value)
  }

  case class Gte(value: String) extends RangeBound {
    val _gte = "gte"

    override def toJson: Map[String, Any] = Map(_gte -> value)
  }

  case class Lt(value: String) extends RangeBound {
    val _lt = "lt"

    override def toJson: Map[String, Any] = Map(_lt -> value)
  }

  case class Lte(value: String) extends RangeBound {
    val _lte = "lte"

    override def toJson: Map[String, Any] = Map(_lte -> value)
  }

  case class WildcardQuery(key: String, value: String) extends Query {
    val _wildcard = "wildcard"

    override def toJson: Map[String, Any] = {
      Map(_wildcard -> Map(key -> value))
    }
  }

  case class TermQuery(key: String, value: String) extends Query {
    val _term = "term"

    override def toJson: Map[String, Any] = {
      Map(_term -> Map(key -> value))
    }
  }

  case class MatchQuery(key: String, value: String) extends Query {
    val _match = "match"

    override def toJson: Map[String, Any] = {
      Map(_match -> Map(key -> value))
    }
  }

  case class PhraseQuery(key: String, value: String) extends Query {
    val _matchPhrase = "match_phrase"

    override def toJson: Map[String, Any] = {
      Map(_matchPhrase -> Map(key -> value))
    }
  }

  case class PrefixQuery(key: String, prefix: String)
    extends Query {
    val _prefix = "prefix"

    override def toJson: Map[String, Any] = {
      Map(_prefix ->
        Map(key-> prefix)
      )
    }
  }

  case class PhrasePrefixQuery(key: String, prefix: String, maxExpansions: Option[Int])
    extends Query {

    val _matchPhrasePrefix = "match_phrase_prefix"
    val _query = "query"
    val _maxExpansions = "max_expansions"

    override def toJson: Map[String, Any] = {
      Map(_matchPhrasePrefix ->
        Map(key->
          (Map(_query -> prefix) ++ maxExpansions.map(_maxExpansions -> _))
        )
      )
    }
  }

  case object MatchAll extends Query {
    val _matchAll = "match_all"
    override def toJson: Map[String, Any] = Map(_matchAll -> Map())
  }

  case class ExistsQuery(field: String) extends Query {
    val _exists = "exists"
    val _field = "field"

    override def toJson: Map[String, Any] = {
      Map(
        _exists -> Map(
          _field -> field
        )
      )
    }
  }
}
