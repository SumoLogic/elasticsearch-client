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
package com.sumologic.elasticsearch.restlastic

import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity, HttpMethod, HttpMethods, HttpRequest, Uri}
import akka.util.Timeout
import com.sumologic.elasticsearch.restlastic.dsl.{Dsl, V2}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class RestlasticSearchClient2(endpointProvider: EndpointProvider, signer: Option[RequestSigner] = None,
                              override val indexExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global,
                              searchExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global)
                             (override implicit val system: ActorSystem = ActorSystem(),
                              override implicit val timeout: Timeout = Timeout(30 seconds))
  extends RestlasticSearchClient(endpointProvider, indexExecutionCtx, searchExecutionCtx) {

  import Dsl._
  import RestlasticSearchClient.ReturnTypes._

  override val version = V2

  def deleteByQuery(index: Index,
                    tpe: Type,
                    deleteQuery: QueryRoot,
                    waitForCompletion: Boolean,
                    proceedOnConflicts: Boolean,
                    refreshAfterDeletion: Boolean,
                    useAutoSlices: Boolean): Future[RawJsonResponse] = {
    deleteByQuery(Seq(index), tpe, deleteQuery, waitForCompletion, proceedOnConflicts, refreshAfterDeletion, useAutoSlices)
  }

  def deleteByQuery(indices: Seq[Index],
                    tpe: Type,
                    deleteQuery: QueryRoot,
                    waitForCompletion: Boolean,
                    proceedOnConflicts: Boolean,
                    refreshAfterDeletion: Boolean,
                    useAutoSlices: Boolean): Future[RawJsonResponse] = {
    implicit val ec = indexExecutionCtx

    Future.reduce(indices.map(i => deleteDocuments(i, tpe, deleteQuery)))(_ ++ _).map(
      resp => RawJsonResponse(resp.toString()))
  }

  def createIndex(index: Index, settings: Option[IndexSetting] = None): Future[RawJsonResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(CreateIndex(settings), s"/${index.name}").recover {
      case ElasticErrorResponse(message, status) if message.toString contains "index_already_exists_exception" =>
        throw IndexAlreadyExistsException(message.toString)
    }
  }

  override def getScript(scriptId: String, lang: String = ""): Future[ScriptResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(EmptyObject, s"/_scripts/$lang/$scriptId", HttpMethods.GET)
      .map(resp => resp.mappedTo[ScriptResponse])
      .recover {
        case _: IllegalArgumentException =>
          ScriptResponse(scriptId, false, None)
      }
  }

  override def addScript(scriptId: String, scriptSource: ScriptSource): Future[AddScriptResponse] = {
    implicit val ec = indexExecutionCtx
    val fut = runEsCommand(scriptSource, s"/_scripts/${scriptSource.lang}/$scriptId", HttpMethods.POST)
    fut.map { resp =>
      val extracted = resp.mappedTo[AddScriptResponse]
      extracted
    }
  }

  override def deleteScript(scriptId: String, lang: String = ""): Future[Boolean] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(EmptyObject, s"/_scripts/$lang/$scriptId", HttpMethods.DELETE)
      .map(_ => true)
      .recover {
        case ex: ElasticErrorResponse if ex.status == 404 => false
      }
  }

  override protected def scrollDelete(index: Index,
                                      tpe: Type,
                                      scrollId: ScrollId,
                                      acc: Map[Index, DeleteResponse],
                                      scrollingFn: (ScrollId) => Future[(ScrollId, SearchResponse)]): Future[Map[Index, DeleteResponse]] = {
    implicit val ec = indexExecutionCtx
    scrollingFn(scrollId).flatMap { case (id, response) =>
      val rawResponse = response.rawSearchResponse
      val documentIds = rawResponse.hits.hits.map(_._id)

      if (documentIds.isEmpty) {
        Future.successful(acc)
      } else {
        bulkDelete(index, tpe, documentIds.map(Document(_, Map())))
          .flatMap { items =>
            scrollDelete(
              index,
              tpe,
              id,
              acc ++ items.map(item => Dsl.Index(item._id) -> DeleteResponse(if (item.success) "deleted" else "error")),
              (scrollId) => scroll(scrollId))
          }
      }
    }
  }

  override def scroll(scrollId: ScrollId, resultWindowOpt: Option[String] = None): Future[(ScrollId, SearchResponse)] = {
    implicit val ec = searchExecutionCtx
    val uriQuery = Uri.Query("scroll_id" -> scrollId.id, "scroll" -> resultWindowOpt.getOrElse(defaultResultWindow))
    runEsCommand(NoOp, s"/_search/scroll", query = uriQuery).map { resp =>
      val sr = resp.mappedTo[SearchResponseWithScrollId]
      (ScrollId(sr._scroll_id), SearchResponse(RawSearchResponse(sr.hits), resp.jsonStr))
    }
  }

  // Scroll requests have optimizations that make them faster when the sort order is _doc.
  // Put sort by _doc in query as described in the the following document
  // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
  override def startScrollRequestIndices(indices: Seq[Index],
                                         tpe: Type,
                                         query: QueryRoot,
                                         resultWindowOpt: Option[String] = None,
                                         fromOpt: Option[Int] = None,
                                         sizeOpt: Option[Int] = None,
                                         preference: Option[String] = None): Future[(ScrollId, SearchResponse)] = {
    val params = Map("scroll" -> resultWindowOpt.getOrElse(defaultResultWindow)) ++
      fromOpt.map("from" -> _.toString) ++
      sizeOpt.map("size" -> _.toString) ++
      preference.map("preference" -> _)
    super.startScrollRequest(indices, tpe, query, resultWindowOpt, fromOpt, sizeOpt, preference, params)
  }

  override def runRawEsRequest(op: String,
                               endpoint: String,
                               method: HttpMethod,
                               query: Uri.Query,
                               contentType: ContentType)
                              (implicit ec: ExecutionContext): Future[RawJsonResponse] = {
    val request = {
      val unauthed = HttpRequest(
        method = method,
        uri = buildUri(endpoint, query),
        entity = HttpEntity(contentType, op.getBytes(StandardCharsets.UTF_8)))
      signer.map(_.withAuthHeader(unauthed)).getOrElse(unauthed)
    }
    super.runRawEsRequest(op, endpoint, method, query, request)
  }

  override def defaultContentType(): ContentType = ContentTypes.`text/plain(UTF-8)`
}
