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
package com.sumologic.elasticsearch.akkahelpers

import akka.NotUsed
import akka.stream.{Graph, SourceShape}
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Source, Unzip}
import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.{ScrollId, SearchResponse}
import com.sumologic.elasticsearch.restlastic.ScrollClient
import com.sumologic.elasticsearch.restlastic.dsl.Dsl._

import scala.concurrent.ExecutionContext

object ScanAndScrollSourceGraph {

  // TODO: Add failure management and shutdown management
  def apply(index: Index, tpe: Type,
            query: QueryRoot,
            scrollSource: ScrollClient,
            sizeOpt: Option[Int] = None)
           (implicit ec: ExecutionContext): Graph[SourceShape[SearchResponse], NotUsed] = {

    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>

      import GraphDSL.Implicits._

      val source: Source[(ScrollId, SearchResponse), NotUsed] = Source.fromFuture(
        scrollSource.startScrollRequest(index, tpe, query, sizeOpt = sizeOpt)
      )

      val merge = builder.add(Merge[(ScrollId, SearchResponse)](2))
      val unzip = builder.add(Unzip[ScrollId, SearchResponse])
      val requestMore = builder.add(Flow[ScrollId].mapAsync(5) { id =>
        scrollSource.scroll(id)
      })

      source ~> merge.in(0)
      merge ~> unzip.in
      unzip.out0 ~> requestMore
      requestMore ~> merge.in(1)

      SourceShape.of(unzip.out1)
    }
  }
}
