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

  sealed trait BoolQuery extends EsOperation

  sealed trait HighlighterType {
    val name: String
  }

  trait Query extends EsOperation

  trait CompoundQuery extends Query

  trait Filter extends EsOperation

  class QueryRoot(query: Query,
                  fromOpt: Option[Int] = None,
                  sizeOpt: Option[Int] = None,
                  sortOpt: Option[Seq[Sort]] = None,
                  timeoutOpt: Option[Int] = None,
                  sourceFilterOpt: Option[Seq[String]] = None,
                  terminateAfterOpt: Option[Int] = None)
      extends RootObject {

    val _query = "query"
    val _size = "size"
    val _sort = "sort"
    val _order = "order"
    val _from = "from"
    val _timeout = "timeout"
    val _source = "_source"
    val _terminate_after = "terminate_after"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_query -> query.toJson(version)) ++
          fromOpt.map(_from -> _) ++
          sizeOpt.map(_size -> _) ++
          timeoutOpt.map(t => _timeout -> s"${t}ms") ++
          sortOpt.map(_sort -> _.map(_.toJson(version))) ++
          sourceFilterOpt.map(_source -> _) ++
          terminateAfterOpt.map(_terminate_after -> _)
    }
  }

  case class ConstantScore(filter: Filter) extends SingleField("constant_score", filter) with Filter


  case class FilteredContext(filter: List[Filter]) extends Query {
    val _filter = "filter"
    val _query = "query"
    val _searchType = "search-type"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(
        _filter -> filter.map(_.toJson(version))
      )
    }
  }

  case class MultiTermFilterContext(filter: Filter*) extends Query {
    val _filter = "filter"
    val _query = "query"
    val _must = "must"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(
        _filter -> Map(_must -> filter.map(_.toJson(version)))
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

    override def toJson(version: EsVersion): Map[String, Any] = {
      version match {
        case V2 => {
          Map(
            _filtered -> Map(
              _query -> query.toJson(version),
              _filter -> Map(_bool -> Map(_must -> filter.map(_.toJson(version))))
            )
          )
        }
        case V6 => {
          Bool(List(Must(query)), FilteredContext(filter.toList)).toJson(version)
        }
      }
    }
  }

  case class TermFilter(term: String, value: String) extends Filter {
    val _term = "term"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_term -> Map(term -> value))
    }
  }

  case class PrefixFilter(field: String, prefix: String) extends Filter {
    val _prefix = "prefix"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_prefix -> Map(field -> prefix))
    }
  }

  case class RegexFilter(field: String, regexp: String) extends Filter {
    val _regexp = "regexp"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_regexp -> Map(field -> regexp))
    }
  }

  case class RangeFilter(key: String, bounds: RangeBound*) extends Filter {
    val _range = "range"

    override def toJson(version: EsVersion): Map[String, Any] = Map(_range -> boundsMap(version))

    private def  boundsMap(version: EsVersion): Map[String, Map[String, Any]] = {
      Map(key -> (bounds :\ Map[String, Any]()) (_.toJson(version) ++ _))
    }
  }

  case class Bool(queries: List[BoolQuery], filterContext: FilteredContext = FilteredContext(List())) extends CompoundQuery {
    val _bool = "bool"

    override def toJson(version: EsVersion): Map[String, Any] = Map(_bool -> (queryMap(version) ++ filterContext.toJson(version)))

    private def queryMap(version: EsVersion): Map[String, Any] = {
      queries.map(_.toJson(version)).map(map => (map.keys.head, map(map.keys.head))).toMap
    }
  }

  case class Should(opts: Query*) extends BoolQuery {
    val _should = "should"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_should -> opts.map(_.toJson(version)))
    }
  }

  case class Must(opts: Query*) extends BoolQuery {
    val _must = "must"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_must -> opts.map(_.toJson(version)))
    }
  }

  case class MustNot(opts: Query*) extends BoolQuery {
    val _mustnot = "must_not"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_mustnot -> opts.map(_.toJson(version)))
    }
  }

  case class RangeQuery(key: String, bounds: RangeBound*) extends Query {
    val _range = "range"

    override def toJson(version: EsVersion): Map[String, Any] = Map(_range -> boundsMap(version))

    private def boundsMap(version: EsVersion): Map[String, Map[String, Any]] = {
      Map(key -> (bounds :\ Map[String, Any]()) (_.toJson(version) ++ _))
    }
  }

  sealed trait RangeBound extends EsOperation

  case class Gt(value: String) extends RangeBound {
    val _gt = "gt"

    override def toJson(version: EsVersion): Map[String, Any] = Map(_gt -> value)
  }

  case class Gte(value: String) extends RangeBound {
    val _gte = "gte"

    override def toJson(version: EsVersion): Map[String, Any] = Map(_gte -> value)
  }

  case class Lt(value: String) extends RangeBound {
    val _lt = "lt"

    override def toJson(version: EsVersion): Map[String, Any] = Map(_lt -> value)
  }

  case class Lte(value: String) extends RangeBound {
    val _lte = "lte"

    override def toJson(version: EsVersion): Map[String, Any] = Map(_lte -> value)
  }

  case class WildcardQuery(key: String, value: String) extends Query {
    val _wildcard = "wildcard"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_wildcard -> Map(key -> value))
    }
  }

  case class RegexQuery(field: String, regexp: String) extends Query {
    val _regexp = "regexp"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_regexp -> Map(field -> regexp))
    }
  }

  case class TermQuery(key: String, value: String) extends Query {
    val _term = "term"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_term -> Map(key -> value))
    }
  }

  case class MatchQuery(key: String, value: String, boost: Double = 1) extends Query {
    val _match = "match"
    val _query = "query"
    val _boost = "boost"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_match ->
          Map(key ->
              Map(_query -> value,
                _boost -> boost)))
    }
  }

  case class PhraseQuery(key: String, value: String) extends Query {
    val _matchPhrase = "match_phrase"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_matchPhrase -> Map(key -> value))
    }
  }

  case class PrefixQuery(key: String, prefix: String)
      extends Query {
    val _prefix = "prefix"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_prefix ->
          Map(key -> prefix)
      )
    }
  }

  case class PhrasePrefixQuery(key: String, prefix: String, maxExpansions: Option[Int])
      extends Query {

    val _matchPhrasePrefix = "match_phrase_prefix"
    val _query = "query"
    val _maxExpansions = "max_expansions"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(_matchPhrasePrefix ->
          Map(key ->
              (Map(_query -> prefix) ++ maxExpansions.map(_maxExpansions -> _))
          )
      )
    }
  }

  case object MatchAll extends Query {
    val _matchAll = "match_all"

    override def toJson(version: EsVersion): Map[String, Any] = Map(_matchAll -> Map())
  }

  case class InnerHits(highlight: Highlight, from: Option[Int] = None, size: Option[Int] = None) extends EsOperation {
    val _from = "from"
    val _size = "size"

    override def toJson(version: EsVersion): Map[String, Any] = highlight.toJson(version) ++
      from.map(_from -> _) ++
      size.map(_size -> _)
  }

  case class NestedQuery(path: String,
                         scoreMode: Option[ScoreMode] = None,
                         query: Bool,
                         innerHits: Option[InnerHits] = None) extends Query {
    val _nested = "nested"
    val _path = "path"
    val _scoreMode = "score_mode"
    val _query = "query"
    val _inner_hits = "inner_hits"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _nested -> innerMap(version)
    )

    private def innerMap(version: EsVersion): Map[String, Any] = Map(
      _path -> path,
      _query -> query.toJson(version)
    ) ++ scoreMode.map(_scoreMode -> _.value) ++ innerHits.map(_inner_hits -> _.toJson(version))
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

  case class MultiMatchQuery(query: String, options: Map[String, String], fields: String*) extends Query {
    val _multiMatch = "multi_match"
    val _query = "query"
    val _fields = "fields"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
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

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _lat -> lat,
      _lon -> lon
    )
  }

  case class GeoDistanceFilter(distance: String, field: String, location: GeoLocation) extends Filter {
    val _geoDistance = "geo_distance"
    val _distance = "distance"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _geoDistance -> Map(
        _distance -> distance,
        field -> location.toJson(version)
      )
    )
  }

  case class DisMaxQuery(queries: Seq[Query], tie_breaker: Option[Float] = None, boost: Option[Float] = None) extends Query {
    val _dis_max = "dis_max"
    val _queries = "queries"
    val _tie_breaker = "tie_breaker"
    val _boost = "boost"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _dis_max -> innerMap(version)
    )

    private def innerMap(version: EsVersion): Map[String, Any] = Map(
      _queries -> queries.map(_.toJson(version))
    ) ++ tie_breaker.map(_tie_breaker -> _) ++ boost.map(_boost -> _)
  }

  case class HighlightRoot(queryRoot: QueryRoot, highlight: Highlight)
      extends RootObject {

    override def toJson(version: EsVersion): Map[String, Any] = {
      queryRoot.toJson(version) ++ highlight.toJson(version)
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

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _highlight -> {
        Map(_fields -> fields.map(_.toJson(version)).reduce(_ ++ _)) ++
            pre_tags ++ post_tags
      }
    )
  }

  case class HighlightField(field: String, highlighter_type: Option[HighlighterType] = None, fragment_size: Option[Int] = None,
                            number_of_fragments: Option[Int] = None, no_match_size: Option[Int] = None, matched_fields: Seq[String] = Seq(),
                            highlight_query: Option[Bool] = None)
      extends EsOperation {
    val _type = "type"
    val _fragment_size = "fragment_size"
    val _number_of_fragments = "number_of_fragments"
    val _no_match_size = "no_match_size"
    val _matched_fields = "matched_fields"
    val _highlight_query = "highlight_query"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      field -> {
        Map[String, Any]() ++
          highlighter_type.map(_type -> _.name) ++
          fragment_size.map(_fragment_size -> _) ++
          number_of_fragments.map(_number_of_fragments -> _) ++
          no_match_size.map(_no_match_size -> _) ++
          matched_fields.map(_matched_fields -> _) ++
          highlight_query.map(_highlight_query -> _.toJson(version))
      }
    )
  }

  /**
    * Elasticsearch postings highlighter. Not supported in Elasticsearch 6.
    */
  case object PostingsHighlighter extends HighlighterType {
    val name = "postings"
  }

  case object PlainHighlighter extends HighlighterType {
    val name = "plain"
  }

  /**
    * Elasticsearch unified highlighter. Not supported in Elasticsearch 2.
    */
  case object UnifiedHighlighter extends HighlighterType {
    val name = "unified"
  }

  case object FastVectorHighlighter extends HighlighterType {
    val name = "fvh"
  }

  case class Scroll(id: String, window: String) extends RootObject {
    override def toJson(version: EsVersion): Map[String, Any] = Map("scroll_id" -> id, "scroll" -> window)
  }

}
