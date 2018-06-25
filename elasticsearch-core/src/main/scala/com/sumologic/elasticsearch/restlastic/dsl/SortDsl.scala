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

import com.sumologic.elasticsearch.restlastic.dsl.Dsl.GeoLocation

//object SortDsl extends SortDsl
trait SortDsl extends DslCommons {

  trait Sort extends EsOperation

  case class SimpleSort(field: String, order: SortOrder) extends Sort {
    val _order = "order"

    override def toJson: Map[String, Any] = Map(field ->
        Map(_order -> order.value)
    )
  }

  case class GeoDistanceSort(field: String, location: GeoLocation, order: SortOrder, unit: String, distanceType: String) extends Sort {
    val _geoDistance = "_geo_distance"
    val _order = "order"
    val _unit = "unit"
    val _distanceType = "distance_type"

    override def toJson: Map[String, Any] = Map(
      _geoDistance -> Map(
        field -> location.toJson,
        _order -> order.value,
        _unit -> unit,
        _distanceType -> distanceType)
    )
  }

  sealed trait SortOrder {
    def value: String
  }

  object SortOrder {
    def fromString(order: String): SortOrder = order match {
      case "asc" => AscSortOrder
      case "desc" => DescSortOrder
    }
  }

  case object AscSortOrder extends SortOrder {
    lazy val value: String = "asc"

    override def toString: String = value
  }

  case object DescSortOrder extends SortOrder {
    lazy val value: String = "desc"

    override def toString: String = value
  }

}