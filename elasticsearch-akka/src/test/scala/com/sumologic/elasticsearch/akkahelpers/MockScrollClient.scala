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

import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes._
import com.sumologic.elasticsearch.restlastic.ScrollClient
import com.sumologic.elasticsearch.restlastic.dsl.Dsl

import scala.concurrent.{ExecutionContext, Future}

class MockScrollClient(results: List[SearchResponse]) extends ScrollClient {
  var id = 1
  var started = false
  var resultsQueue = results
  override val indexExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global
  override def startScrollRequest(index: Dsl.Index, tpe: Dsl.Type, query: Dsl.QueryRoot,
                                  resultWindowOpt: Option[String] = None, fromOpt: Option[Int] = None, sizeOpt: Option[Int] = None): Future[(ScrollId, SearchResponse)] = {
    if (!started) {
      started = true
      processRequest()
    } else {
      Future.failed(new RuntimeException("Scroll already started"))
    }
  }

  override def scroll(scrollId: ScrollId, resultWindowOpt: Option[String] = None): Future[(ScrollId, SearchResponse)] = {
    if (scrollId.id.toInt == id) {
      processRequest()
    } else {
      Future.failed(new RuntimeException("Invalid id"))
    }

  }

  private def processRequest(): Future[(ScrollId, SearchResponse)] = {
    id += 1
    resultsQueue match {
      case head :: rest =>
        resultsQueue = rest
        Future.successful((ScrollId(id.toString), head))
      case Nil =>
        Future.successful((ScrollId(id.toString), SearchResponse.empty))
    }
  }
}

