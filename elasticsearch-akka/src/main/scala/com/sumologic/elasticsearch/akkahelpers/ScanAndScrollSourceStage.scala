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

import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes
import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.SearchResponse
import com.sumologic.elasticsearch.restlastic.ScrollClient
import com.sumologic.elasticsearch.restlastic.dsl.Dsl._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ScanAndScrollSourceStage(name: String = "UnnamedScanAndScrollSourceStage",
                               index: Index,
                               tpe: Type,
                               query: QueryRoot,
                               scrollSource: ScrollClient,
                               sizeOpt: Option[Int],
                               timeout: Duration = 10.seconds) (implicit val ec: ExecutionContext)
  extends GraphStage[SourceShape[SearchResponse]] {

  import ScanAndScrollSourceStage._

  val outlet: Outlet[SearchResponse] = Outlet[SearchResponse](s"$name.out")

  override val shape: SourceShape[SearchResponse] = SourceShape.of(outlet)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {

      private var resultFuture: Future[Either[Throwable, IdAndData]] = _

      override def preStart(): Unit = {
        resultFuture = scrollSource.startScrollRequest(index, tpe, query, sizeOpt = sizeOpt).map {
          case (id, data) => Right(IdAndData(id, data))
        }.recover(recovery)
      }

      override def onPull(): Unit = {
        val result = Await.result(resultFuture, timeout)

        result match {
          case Left(ex) => failStage(ex)
          case Right(IdAndData(scrollId, searchResponse)) =>
            resultFuture = scrollSource.scroll(scrollId).map {
              case (id, data) => Right(IdAndData(id, data))
            }.recover(recovery)

            if (searchResponse.length == 0) {
              completeStage()
            } else {
              push(outlet, searchResponse)
            }
        }
      }

      private val recovery: PartialFunction[Throwable, Either[Throwable, IdAndData]] = {
        case ex => Left(ScanAndScrollFailureException("Scroll future failed with exception", ex))
      }

      setHandler(outlet, this)
    }

}

object ScanAndScrollSourceStage {
  private case class IdAndData(id: ReturnTypes.ScrollId, data: SearchResponse)
  case class ScanAndScrollFailureException(message: String, cause: Throwable) extends Exception(message, cause)
}