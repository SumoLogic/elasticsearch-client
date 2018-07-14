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
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Source, Unzip}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.{ScrollId, SearchResponse}
import com.sumologic.elasticsearch.restlastic.ScrollClient
import com.sumologic.elasticsearch.restlastic.dsl.Dsl._

import scala.concurrent.ExecutionContext

object ScanAndScrollSourceGraph {

  // TODO: Add failure management
  def apply(index: Index, tpe: Type,
            query: QueryRoot,
            scrollSource: ScrollClient,
            sizeOpt: Option[Int] = None)
           (implicit ec: ExecutionContext): Graph[SourceShape[SearchResponse], NotUsed] = {

    val completionDeciderGraph = new GraphStage[FlowShape[(ScrollId, SearchResponse), (ScrollId, SearchResponse)]] {
      val in: Inlet[(ScrollId, SearchResponse)] = Inlet[(ScrollId, SearchResponse)]("completionDecider.in")
      val out: Outlet[(ScrollId, SearchResponse)] = Outlet[(ScrollId, SearchResponse)]("completionDecider.out")
      override val shape: FlowShape[(ScrollId, SearchResponse), (ScrollId, SearchResponse)] = FlowShape.of(in, out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) with InHandler with OutHandler {
          override def onPull(): Unit = {
            pull(in)
          }

          override def onPush(): Unit = {
            val elem = grab(in)
            if (elem._2.length == 0) {
              completeStage()
            } else {
              push(out, elem)
            }
          }

          setHandlers(in, out, this)
        }
    }

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
      val completionDecider = builder.add(Flow.fromGraph(completionDeciderGraph))

      source ~> merge.in(0)
      merge ~> completionDecider ~> unzip.in
      unzip.out0 ~> requestMore
      requestMore ~> merge.in(1)

      SourceShape.of(unzip.out1)
    }
  }
}
