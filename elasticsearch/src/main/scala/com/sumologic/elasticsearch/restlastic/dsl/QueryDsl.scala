package com.sumologic.elasticsearch.restlastic.dsl

trait QueryDsl extends DslCommons {

  trait Query extends EsOperation

  sealed trait BoolQuery extends EsOperation

  sealed trait Filter extends EsOperation

  case class QueryRoot(query: Query, sizeOpt: Option[Int] = None)  extends RootObject {
    val _query = "query"
    val _size = "size"
    override def toJson: Map[String, Any] = {
      val terms = _query -> query.toJson :: sizeOpt.map(size => _size -> size).toList
      terms.toMap
    }
  }

  case class ConstantScore(filter: Filter) extends SingleField("constant_score", filter) with Filter

  case class FilteredQuery(filter: Filter, query: Query) extends Query {
    val _filtered = "filtered"
    val _filter = "filter"
    val _query = "query"

    override def toJson: Map[String, Any] = {
      Map(
        _filtered -> Map(
          _query -> query.toJson,
          _filter -> filter.toJson
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

  case object MatchAll extends Query {
    val _matchAll = "match_all"
    override def toJson: Map[String, Any] = Map(_matchAll -> Map())
  }
}


