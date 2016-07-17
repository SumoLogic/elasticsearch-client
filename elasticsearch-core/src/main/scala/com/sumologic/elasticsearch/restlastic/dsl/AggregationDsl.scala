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

  case class AggregationQuery(query: Query, aggs: Aggregation)
    extends Query with RootObject {

    val _query = "query"
    val _aggs = "aggs"
    val _size = "size"

    override def toJson: Map[String, Any] = {
      Map(_query -> query.toJson,
        _aggs -> aggs.toJson,
        _size -> 0)
    }
  }

  case class TermsAggregation(field: String, include: Option[String],
                              size: Option[Int], shardSize: Option[Int])
    extends Aggregation {

    val _aggsName = "aggs_name"
    val _terms = "terms"
    val _field = "field"
    val _include = "include"
    val _size = "size"
    val _shardSize = "shard_size"

    override def toJson: Map[String, Any] = {
      Map(_aggsName ->
        Map(_terms ->
          (Map(_field -> field)
            ++ include.map(_include -> _)
            ++ size.map(_size -> _)
            ++ shardSize.map(_shardSize -> _))
        )
      )
    }
  }

}
