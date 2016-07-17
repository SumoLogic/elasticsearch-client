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

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.{ScrollId, SearchResponse}
import com.sumologic.elasticsearch.restlastic.dsl.Dsl
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory
import spray.can.Http
import spray.http.HttpMethods._
import spray.http.Uri.{Query => UriQuery}
import spray.http.{HttpResponse, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

trait ScrollClient {
  import Dsl._
  def startScrollRequest(index: Index, tpe: Type, query: QueryRoot, resultWindow: String = "1m"): Future[ScrollId]
  def scroll(scrollId: ScrollId, resultWindow: String = "1m"): Future[(ScrollId, SearchResponse)]
}

case class Endpoint(host: String, port: Int)
trait EndpointProvider {
  def endpoint: Endpoint
  def ready: Boolean
}

class StaticEndpoint(_endpoint: Endpoint) extends EndpointProvider {
  override def endpoint: Endpoint = _endpoint
  override def ready = true
}

trait RequestSigner {
  def withAuthHeader(httpRequest: HttpRequest): HttpRequest
}

/**
 * The RestlasticSearchClient is an implementation of a subset of the ElasticSearch protocol using the REST client
 * instead of the native client. The DSL classes provide a (relatively) typesafe mapping from scala code to the JSON
 * used by ElasticSearch.
 * @param endpointProvider EndpointProvider
 */
class RestlasticSearchClient(endpointProvider: EndpointProvider, signer: Option[RequestSigner] = None,
                             indexExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global,
                             searchExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global)
  extends ScrollClient {

  implicit val system: ActorSystem = ActorSystem()
  implicit val timeout: Timeout = Timeout(30.seconds)
  private implicit val formats = org.json4s.DefaultFormats
  private val logger = LoggerFactory.getLogger(RestlasticSearchClient.getClass)
  import Dsl._
  import RestlasticSearchClient.ReturnTypes._

  def ready: Boolean = endpointProvider.ready
  def query(index: Index, tpe: Type, query: QueryRoot): Future[SearchResponse] = {
    implicit val ec = searchExecutionCtx
    runEsCommand(query, s"/${index.name}/${tpe.name}/_search").map { rawJson =>
      SearchResponse(rawJson.mappedTo[RawSearchResponse], rawJson.jsonStr)
    }
  }

  def bucketAggregation(index: Index, tpe: Type, query: AggregationQuery): Future[BucketAggregationResultBody] = {
    implicit val ec = searchExecutionCtx
    runEsCommand(query, s"/${index.name}/${tpe.name}/_search").map { rawJson =>
      rawJson.mappedTo[BucketAggregationResponse].aggregations.aggs_name
    }
  }

  def suggest(index: Index, tpe: Type, query: Suggest): Future[List[String]] = {
    // I'm not totally sure why, but you don't specify the type for _suggest queries
    implicit val ec = searchExecutionCtx
    val fut = runEsCommand(query, s"/${index.name}/_suggest")
    fut.map { resp =>
      val extracted = resp.mappedTo[SuggestResult]
      extracted.suggestions
    }
  }

  def count(index: Index, tpe: Type, query: QueryRoot): Future[Int] = {
    implicit val ec = searchExecutionCtx
    val fut = runEsCommand(query, s"/${index.name}/${tpe.name}/_count")
    fut.map(_.mappedTo[CountResponse].count)
  }

  def index(index: Index, tpe: Type, doc: Document): Future[IndexResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(doc, s"/${index.name}/${tpe.name}/${doc.id}").map(_.mappedTo[IndexResponse])

  }

  def bulkIndex(bulk: Bulk): Future[Seq[BulkItem]] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(bulk, s"/_bulk").map { resp =>
      val bulkResp = resp.mappedTo[BulkIndexResponse]
      bulkResp.items.map(_.values.head)
    }
  }

  def bulkIndex(index: Index, tpe: Type, documents: Seq[Document]): Future[Seq[BulkItem]] = {
    val bulkOperation = Bulk(documents.map(BulkOperation(create, Some(index -> tpe), _)))
    bulkIndex(bulkOperation)
  }

  def bulkUpdate(index: Index, tpe: Type, documents: Seq[Document]): Future[Seq[BulkItem]] = {
    val bulkOperation = Bulk(documents.map(BulkOperation(update, Some(index -> tpe), _)))
    bulkIndex(bulkOperation)
  }

  def putMapping(index: Index, tpe: Type, mapping: Mapping): Future[RawJsonResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(mapping, s"/${index.name}/_mapping/${tpe.name}")
  }

  def getMapping(index: Index, tpe: Type): Future[RawJsonResponse] = {
    implicit val ec = searchExecutionCtx
    runEsCommand(EmptyObject, s"/${index.name}/_mapping/${tpe.name}", GET)
  }

  def createIndex(index: Index, settings: Option[IndexSetting] = None): Future[RawJsonResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(CreateIndex(settings), s"/${index.name}").recover {
      case ElasticErrorResponse(message, status) if message contains "IndexAlreadyExistsException" =>
        throw new IndexAlreadyExistsException(message)
    }
  }

  def deleteIndex(index: Index): Future[RawJsonResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(EmptyObject, s"/${index.name}", DELETE)
  }

  def deleteDocument(index: Index, tpe: Type, query: QueryRoot): Future[RawJsonResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(query, s"/${index.name}/${tpe.name}/_query", DELETE)
  }

  def startScrollRequest(index: Index, tpe: Type, query: QueryRoot, resultWindow: String = "1m"): Future[ScrollId] = {
    implicit val ec = searchExecutionCtx
    val uriQuery = UriQuery("scroll" -> resultWindow, "search_type" -> "scan")
    runEsCommand(query, s"/${index.name}/${tpe.name}/_search", query = uriQuery).map { resp =>
      val parsed = resp.mappedTo[ScrollResponse]
      ScrollId(parsed._scroll_id)
    }
  }

  def scroll(scrollId: ScrollId, resultWindow: String = "1m"): Future[(ScrollId, SearchResponse)] = {
    implicit val ec = searchExecutionCtx
    val uriQuery = UriQuery("scroll_id" -> scrollId.id, "scroll" -> resultWindow)
    runEsCommand(NoOp, s"/_search/scroll", query = uriQuery).map { resp =>
      val sr = resp.mappedTo[SearchResponseWithScrollId]
      (ScrollId(sr._scroll_id), SearchResponse(RawSearchResponse(sr.hits), resp.jsonStr))
    }
  }

  def flush(index: Index): Future[RawJsonResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(EmptyObject, s"/${index.name}/_flush")
  }

  def refresh(index: Index): Future[RawJsonResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(EmptyObject, s"/${index.name}/_refresh")
  }

  private def runEsCommand(op: RootObject,
                           endpoint: String,
                           method: HttpMethod = POST,
                           query: UriQuery = UriQuery.Empty)
                          (implicit ec: ExecutionContext): Future[RawJsonResponse] = {
    runRawEsRequest(op.toJsonStr, endpoint, method, query)
  }

  def runRawEsRequest(op: String,
                      endpoint: String,
                      method: HttpMethod = POST,
                      query: UriQuery = UriQuery.Empty)
                     (implicit ec: ExecutionContext = ExecutionContext.Implicits.global): Future[RawJsonResponse] = {
    val request = {
      val unauthed = HttpRequest(
        method = method,
        uri = buildUri(endpoint, query),
        entity = HttpEntity(op))
      signer.map(_.withAuthHeader(unauthed)).getOrElse(unauthed)
    }

    val responseFuture: Future[HttpResponse] = (IO(Http) ? request).mapTo[HttpResponse]

    responseFuture.map { response =>
      logger.debug(s"Got Es response: ${response.status}")
      if (response.status.isFailure) {
        logger.warn(s"Failure response: ${response.entity.asString.take(500)}")
        logger.warn(s"Failing request: ${op.take(5000)}")

        val jsonTree = parse(response.entity.asString)
        throw jsonTree.extract[ElasticErrorResponse]
      }
      RawJsonResponse(response.entity.asString)
    }
  }

  private def buildUri(path: String, query: UriQuery = UriQuery.Empty): Uri = {
    val ep = endpointProvider.endpoint
    val scheme = ep.port match {
      case 443 => "https"
      case _ => "http"
    }
    Uri.from(scheme = scheme, host = ep.host, port = ep.port, path = path, query = query)
  }
}

object RestlasticSearchClient {

  object ReturnTypes {

    case class ScrollId(id: String)

    case class BulkIndexResponse(items: List[Map[String, BulkItem]])
    case class BulkItem(_index: String, _type: String, _id: String, status: Int, error: Option[String]) {
      def created = status > 200 && status < 299 && !alreadyExists
      def alreadyExists = error.exists(_.contains("DocumentAlreadyExists"))
    }

    case class SearchResponse(rawSearchResponse: RawSearchResponse, jsonStr: String) {
      def extractSource[T: Manifest]: Seq[T] = {
        rawSearchResponse.extractSource[T]
      }

      def sourceAsMap: Seq[Map[String, Any]] = rawSearchResponse.sourceAsMap
      def length = rawSearchResponse.hits.hits.length
    }

    object SearchResponse {
      val empty = SearchResponse(RawSearchResponse(Hits(List())), "{}")
    }

    case class CountResponse(count: Int)

    case class SearchResponseWithScrollId(_scroll_id: String, hits: Hits)
    case class RawSearchResponse(hits: Hits) {
      private implicit val formats = org.json4s.DefaultFormats
      def extractSource[T: Manifest]: Seq[T] = {
        hits.hits.map(_._source.extract[T])
      }

      def sourceAsMap: Seq[Map[String, Any]] = hits.hits.map(_._source.values)
    }

    case class BucketAggregationResponse(aggregations: Aggregations)
    case class Aggregations(aggs_name: BucketAggregationResultBody )
    case class BucketAggregationResultBody(doc_count_error_upper_bound: Int,
                                           sum_other_doc_count: Int,
                                           buckets: List[Bucket])
    case class Bucket(key: String, doc_count: Int)

    case class Hits(hits: List[ElasticJsonDocument], total: Int = 0)
    case class ElasticJsonDocument(_index: String,
                                   _type: String,
                                   _id: String,
                                    _score: Option[Float],
                                   _source: JObject)

    case class RawJsonResponse(jsonStr: String) {
      private implicit val formats = org.json4s.DefaultFormats
      def mappedTo[T: Manifest]: T = {
        val jsonTree = parse(jsonStr)
        jsonTree.extract[T]
      }
    }

    case class SuggestResult(suggest: List[Suggestion]) {
      def suggestions: List[String] = suggest.flatMap(_.options.map(_.text))
    }

    case class Suggestion(text: String, options: List[SuggestOption])

    case class SuggestOption(text: String, score: Float)

    case class IndexResponse(created: Boolean)

    case class IndexAlreadyExistsException(message: String) extends Exception(message)

    case class ElasticErrorResponse(error: String, status: Int) extends Exception(s"ElasticsearchError(status=$status): $error")

    case class ScrollResponse(_scroll_id: String)

  }

}
