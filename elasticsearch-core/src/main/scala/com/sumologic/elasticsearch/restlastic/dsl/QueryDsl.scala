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

trait QueryDsl extends DslCommons with SortDsl {

  trait Query extends EsOperation

  sealed trait BoolQuery extends EsOperation

  trait Filter extends EsOperation

  trait QueryRootBase extends RootObject

  case class QueryRoot(query: Query,
                       fromOpt: Option[Int],
                       sizeOpt: Option[Int],
                       sort: Seq[Sort],
                       timeout: Option[Int],
                       sourceFilter: Option[Seq[String]])
    extends QueryRootBase {

    val _query = "query"
    val _size = "size"
    val _sort = "sort"
    val _order = "order"
    val _from = "from"
    val _timeout = "timeout"
    val _source = "_source"

    override def toJson: Map[String, Any] = {
      Map(_query -> query.toJson) ++
        fromOpt.map(_from -> _) ++
        sizeOpt.map(_size -> _) ++
        timeout.map(t => _timeout -> s"${t}ms") ++
        sort.map(_sort -> _.toJson) ++
        sourceFilter.map(_source -> _)
    }
  }

  object QueryRoot {

    def apply(query: Query,
              fromOpt: Option[Int] = None,
              sizeOpt: Option[Int] = None,
              sortOpt: Option[Seq[(String, String)]] = None,
              timeout: Option[Int] = None,
              sourceFilter: Option[Seq[String]] = None): QueryRoot = {
      val sorts = sortOpt
        .map(_.foldLeft(Seq.empty[Sort])((sorts, value) => sorts :+ SimpleSort(value._1, SortOrder.fromString(value._2))))
        .getOrElse(Nil)
      new QueryRoot(query, fromOpt, sizeOpt, sorts, timeout, sourceFilter)
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

  case class RegexFilter(field: String, regexp: String) extends Filter {
    val _regexp = "regexp"

    override def toJson: Map[String, Any] = {
      Map(_regexp -> Map(field -> regexp))
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

  case class RegexQuery(field: String, regexp: String) extends Query {
    val _regexp = "regexp"

    override def toJson: Map[String, Any] = {
      Map(_regexp -> Map(field -> regexp))
    }
  }

  case class TermQuery(key: String, value: String) extends Query {
    val _term = "term"

    override def toJson: Map[String, Any] = {
      Map(_term -> Map(key -> value))
    }
  }

  case class MatchQuery(key: String, value: String, boost: Double = 1) extends Query {
    val _match = "match"
    val _query = "query"
    val _boost = "boost"

    override def toJson: Map[String, Any] = {
      Map(_match ->
        Map(key ->
          Map(_query -> value,
              _boost -> boost)))
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
  
  case class NestedQuery(path: String, scoreMode: Option[ScoreMode] = None, query: Bool) extends Query {
    val _nested = "nested"
    val _path = "path"
    val _scoreMode = "score_mode"
    val _query = "query"

    lazy val innerMap: Map[String, Any] = Map(
      _path -> path,
      _query -> query.toJson
    ) ++ scoreMode.map(_scoreMode -> _.value)

    override def toJson: Map[String, Any] = Map(
      _nested -> innerMap
    )
  }

  sealed trait ScoreMode {
    def value: String
  }

  case object AvgScoreMode extends ScoreMode {
    override def value: String = "avg"
  }

  case object MaxScoreMode extends ScoreMode {
    override def value: String = "max"
  }

  case object SumScoreMode extends ScoreMode {
    override def value: String = "sum"
  }

  case object NoneScoreMode extends ScoreMode {
    override def value: String = "none"
  }

  case class MultiMatchQuery(query: String, fields: String*) extends Query {
    val _multiMatch = "multi_match"
    val _query = "query"
    val _fields = "fields"

    override def toJson: Map[String, Any] = Map(
      _multiMatch -> Map(
        _query -> query,
        _fields -> fields.toList
      )
    )
  }

  case class MultiMatchQueryWithOptions(query: String, options: Map[String, String], fields: String*) extends Query {
    val _multiMatch = "multi_match"
    val _query = "query"
    val _fields = "fields"

    override def toJson: Map[String, Any] = Map(
      _multiMatch -> (Map(
        _query -> query,
        _fields -> fields.toList) ++
        options
      )
    )
  }

  case class GeoLocation(lat: Double, lon: Double) extends Query {
    val _lat = "lat"
    val _lon = "lon"

    override def toJson: Map[String, Any] = Map(
      _lat -> lat,
      _lon -> lon
    )
  }

  case class GeoDistanceFilter(distance: String, field: String, location: GeoLocation) extends Filter {
    val _geoDistance = "geo_distance"
    val _distance = "distance"

    override def toJson: Map[String, Any] = Map(
      _geoDistance -> Map(
        _distance -> distance,
        field -> location.toJson
      )
    )
  }

  case class DisMaxQuery(queries: Seq[Query], tie_breaker: Option[Float] = None, boost: Option[Float] = None) extends Query {
    val _dis_max = "dis_max"
    val _queries = "queries"
    val _tie_breaker = "tie_breaker"
    val _boost = "boost"

    lazy val innerMap: Map[String, Any] = Map(
      _queries -> queries.map(_.toJson)
    ) ++ tie_breaker.map(_tie_breaker -> _) ++ boost.map(_boost -> _)

    override def toJson: Map[String, Any] = Map(
      _dis_max -> innerMap
    )
  }

  case class HighlightRoot(queryRoot: QueryRoot, highlight: Highlight)
    extends QueryRootBase {

    override def toJson: Map[String, Any] = {
      queryRoot.toJson ++ highlight.toJson
    }
  }

  case class Highlight(fields: Seq[HighlightField], preTags: Seq[String] = Seq(), postTags: Seq[String] = Seq())
    extends EsOperation {

    val _pre_tags = "pre_tags"
    val _post_tags = "post_tags"
    val _fields = "fields"
    val _highlight = "highlight"

    val pre_tags = if (preTags.isEmpty) Map[String, Any]() else Map(_pre_tags -> preTags)
    val post_tags = if (postTags.isEmpty) Map[String, Any]() else Map(_post_tags -> postTags)

    override def toJson: Map[String, Any] = Map(
      _highlight -> {
        Map( _fields -> fields.foldLeft(Map[String, Any]())((l, r) => l ++ r.toJson)) ++
          pre_tags ++ post_tags
      }
    )
  }

  case class HighlightField(field: String, highlighter_type: Option[HighlighterType] = None, fragment_size: Option[Int] = None,
                            number_of_fragments: Option[Int] = None, no_match_size: Option[Int] = None, matched_fields: Seq[String] = Seq())
    extends EsOperation {
    val _type = "type"
    val _fragment_size = "fragment_size"
    val _number_of_fragments = "number_of_fragments"
    val _no_match_size = "no_match_size"
    val _matched_fields = "matched_fields"

    override def toJson: Map[String, Any] = Map(
      field -> {
        Map[String, Any]() ++
          highlighter_type.map(_type -> _.name) ++
          fragment_size.map(_fragment_size -> _) ++
          number_of_fragments.map(_number_of_fragments -> _) ++
          no_match_size.map(_no_match_size -> _) ++
          matched_fields.map(_matched_fields -> _)
      }
    )
  }

  case object PlainHighlighter extends HighlighterType {
    val name = "plain"
  }

  case object PostingsHighlighter extends HighlighterType {
    val name = "postings"
  }

  case object FastVectorHighlighter extends HighlighterType {
    val name = "fvh"
  }
}
