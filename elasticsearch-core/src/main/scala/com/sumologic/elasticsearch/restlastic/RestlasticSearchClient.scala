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

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

trait ScrollClient {

  import Dsl._

  val defaultResultWindow = "1m"
  val indexExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global

  def startScrollRequest(index: Index, tpe: Type, query: QueryRoot, resultWindowOpt: Option[String] = None, sizeOpt: Option[Int] = None): Future[(ScrollId, SearchResponse)]

  def scroll(scrollId: ScrollId, resultWindowOpt: Option[String] = None): Future[(ScrollId, SearchResponse)]
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
  *
  * @param endpointProvider EndpointProvider
  */
class RestlasticSearchClient(endpointProvider: EndpointProvider, signer: Option[RequestSigner] = None,
                             override val indexExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global,
                             searchExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global)
                            (implicit val system: ActorSystem = ActorSystem(), val timeout: Timeout = Timeout(30.seconds))
    extends ScrollClient {

  private implicit val formats = org.json4s.DefaultFormats
  private val logger = LoggerFactory.getLogger(RestlasticSearchClient.getClass)

  import Dsl._
  import RestlasticSearchClient.ReturnTypes._

  def ready: Boolean = endpointProvider.ready

  def query(index: Index, tpe: Type, query: RootObject, rawJsonStr: Boolean = true, uriQuery: UriQuery = UriQuery.Empty): Future[SearchResponse] = {
    implicit val ec = searchExecutionCtx
    runEsCommand(query, s"/${index.name}/${tpe.name}/_search", query = uriQuery).map { rawJson =>
      val jsonStr = if (rawJsonStr) rawJson.jsonStr else ""
      SearchResponse(rawJson.mappedTo[RawSearchResponse], jsonStr)
    }
  }

  def bucketNestedAggregation(index: Index, tpe: Type, query: AggregationQuery): Future[BucketNested] = {
    implicit val ec = searchExecutionCtx
    runEsCommand(query, s"/${index.name}/${tpe.name}/_search").map { rawJson =>
      BucketNested(rawJson.mappedTo[BucketNestedAggregationResponse].aggregations._2)
    }
  }

  def bucketAggregation(index: Index, tpe: Type, query: AggregationQuery): Future[BucketAggregationResultBody] = {
    implicit val ec = searchExecutionCtx
    runEsCommand(query, s"/${index.name}/${tpe.name}/_search").map { rawJson =>
      rawJson.mappedTo[BucketAggregationResponse].aggregations.aggs_name
    }
  }

  def suggest(index: Index, tpe: Type, query: SuggestRoot): Future[Map[String,List[String]]] = {
    // I'm not totally sure why, but you don't specify the type for _suggest queries
    implicit val ec = searchExecutionCtx
    val fut = runEsCommand(query, s"/${index.name}/_search")
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

  def deleteById(index: Index, tpe: Type, id: String): Future[DeleteResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(NoOp, s"/${index.name}/${tpe.name}/$id", DELETE).map(_.mappedTo[DeleteResponse])
  }

  def documentExistsById(index: Index, tpe: Type, id: String): Future[Boolean] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(NoOp, s"/${index.name}/${tpe.name}/$id", HEAD).map(_ => true).recover {
      case ex: ElasticErrorResponse if ex.status == 404 =>
        false
    }
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

  // retryOnConflictOpt specifies how many times to retry before throwing version conflict exception.
  // https://www.elastic.co/guide/en/elasticsearch/reference/2.3/docs-update.html#_parameters_2
  def bulkUpdate(index: Index, tpe: Type, documents: Seq[Document], retryOnConflictOpt: Option[Int] = None): Future[Seq[BulkItem]] = {
    val bulkOperation = Bulk(documents.map(BulkOperation(update, Some(index -> tpe), _, retryOnConflictOpt)))
    bulkIndex(bulkOperation)
  }

  def bulkDelete(index: Index, tpe: Type, documents: Seq[Document]): Future[Seq[BulkItem]] = {
    val bulkOperation = Bulk(documents.map(BulkOperation(delete, Some(index -> tpe), _)))
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
    runEsCommand(CreateIndex(settings), s"/${index.name}", PUT).recover {
      case ElasticErrorResponse(message, status) if message.toString contains "resource_already_exists_exception" =>
        throw IndexAlreadyExistsException(message.toString)
    }
  }

  def deleteIndex(index: Index): Future[RawJsonResponse] = {
    implicit val ec = indexExecutionCtx
    runEsCommand(EmptyObject, s"/${index.name}", DELETE)
  }

  @deprecated("When plugin is not enabled this function doesn't handle pagination, so it deletes only first page of query results. Replaced by deleteDocuments.")
  def deleteDocument(index: Index, tpe: Type, deleteQuery: QueryRoot, pluginEnabled: Boolean = false): Future[RawJsonResponse] = {
    implicit val ec = indexExecutionCtx
    if (pluginEnabled) {
      runEsCommand(deleteQuery, s"/${index.name}/${tpe.name}/_query", DELETE)
    } else {
      val response = Await.result(query(index, tpe, deleteQuery, rawJsonStr = false), 10.seconds).rawSearchResponse
      val totalHits = response.hits.total
      val documents = response.hits.hits.map(_._id)
      if (totalHits > documents.length) {
        logger.warn(s"deleting only first ${documents.length}/$totalHits matches. " +
            "Use deleteDocuments, if you want to delete more at once.")
      }

      bulkDelete(index, tpe, documents.map(Document(_, Map()))).map(res => RawJsonResponse(res.toString))
    }
  }

  def deleteDocuments(index: Index, tpe: Type, deleteQuery: QueryRoot, pluginEnabled: Boolean = false): Future[Map[Index, DeleteResponse]] = {
    def firstScroll(scId: ScrollId) = startScrollRequest(index, tpe, deleteQuery)

    scrollDelete(index, tpe, ScrollId(""), Map.empty[Index, DeleteResponse], firstScroll)
  }

  private def scrollDelete(index: Index,
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
                acc ++ items.map(item => Dsl.Index(item._id) -> DeleteResponse("deleted")),
                (scrollId) => scroll(scrollId))
            }
      }
    }
  }

  // Scroll requests have optimizations that make them faster when the sort order is _doc.
  // Put sort by _doc in query as described in the the following document
  // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
  def startScrollRequest(index: Index, tpe: Type, query: QueryRoot, resultWindowOpt: Option[String] = None, sizeOpt: Option[Int] = None): Future[(ScrollId, SearchResponse)] = {
    implicit val ec = searchExecutionCtx
    val params = Map("scroll" -> resultWindowOpt.getOrElse(defaultResultWindow)) ++ sizeOpt.map("size" -> _.toString)
    runEsCommand(query, s"/${index.name}/${tpe.name}/_search", query = UriQuery(params)).map { resp =>
      val sr = resp.mappedTo[SearchResponseWithScrollId]
      (ScrollId(sr._scroll_id), SearchResponse(RawSearchResponse(sr.hits), resp.jsonStr))
    }
  }

  def scroll(scrollId: ScrollId, resultWindowOpt: Option[String] = None): Future[(ScrollId, SearchResponse)] = {
    implicit val ec = searchExecutionCtx
    val uriQuery = UriQuery("scroll_id" -> scrollId.id, "scroll" -> resultWindowOpt.getOrElse(defaultResultWindow))
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
        entity = HttpEntity(ContentTypes.`application/json`, op),
        headers = List())
      signer.map(_.withAuthHeader(unauthed)).getOrElse(unauthed)
    }

    logger.debug(f"Got Rs request: $request (op was $op)")

    //println(request)
    val responseFuture: Future[HttpResponse] = (IO(Http) ? request).mapTo[HttpResponse]

    responseFuture.map { response =>
      logger.debug(f"Got Es response: $response")
      if (response.status.isFailure) {
        logger.warn(s"Failure response: ${response.entity.asString.take(500)}")
        logger.warn(s"Failing request: ${op.take(5000)}")
        throw ElasticErrorResponse(JString(response.entity.asString), response.status.intValue)
      }
      val result = RawJsonResponse(response.entity.asString)
      //println(response)
      result
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

    case class BulkIndexError(reason: String)

    case class BulkItem(_index: String, _type: String, _id: String, status: Int, error: Option[BulkIndexError]) {
      def created = status > 200 && status < 299 && !alreadyExists

      def alreadyExists = error.exists(_.reason.contains("document already exists"))

      def success = status >= 200 && status <= 299
    }

    case class SearchResponse(rawSearchResponse: RawSearchResponse, jsonStr: String) {
      def extractSource[T: Manifest]: Seq[T] = {
        rawSearchResponse.extractSource[T]
      }

      def sourceAsMap: Seq[Map[String, Any]] = rawSearchResponse.sourceAsMap

      def highlightAsMaps: Seq[Map[String, Any]] = rawSearchResponse.highlightAsMaps

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

      def highlightAsMaps: Seq[Map[String, Any]] = hits.hits.flatMap(_.highlight.map(_.values))
    }

    case class BucketNested(underlying: BucketNestedMap)

    type BucketNestedMap = Map[String, Any]
    type NestedAggregations = (String, BucketNestedMap)

    case class BucketNestedAggregationResponse(aggregations: NestedAggregations)

    case class BucketNestedAggregationResultBody(doc_count_error_upper_bound: Int,
                                                 sum_other_doc_count: Int,
                                                 buckets: List[BucketNestedMap])


    case class BucketAggregationResponse(aggregations: Aggregations)

    case class Aggregations(aggs_name: BucketAggregationResultBody)

    case class BucketAggregationResultBody(doc_count_error_upper_bound: Int,
                                           sum_other_doc_count: Int,
                                           buckets: List[Bucket])

    case class Bucket(key: String, doc_count: Int)

    case class Hits(hits: List[ElasticJsonDocument], total: Int = 0)

    case class ElasticJsonDocument(_index: String,
                                   _type: String,
                                   _id: String,
                                   _score: Option[Float],
                                   _source: JObject,
                                   highlight: Option[JObject])

    case class RawJsonResponse(jsonStr: String) {
      private implicit val formats = org.json4s.DefaultFormats

      def mappedTo[T: Manifest]: T = {
        val jsonTree = parse(jsonStr)
        jsonTree.extract[T]
      }
    }

    case class SuggestResult(suggest: Map[String, List[Suggestion]]) {
      def suggestions: Map[String, List[String]] = suggest.map{ case (name, suggestions) => name -> suggestions.flatMap(_.options.map(_.text))}
    }

    case class Suggestion(text: String, options: List[SuggestOption])

    case class SuggestOption(text: String, _score: Float)

    case class IndexResponse(result: String) {
      def isSuccess = result == IndexApiResponse.Created.toString
    }

    case class DeleteResponse(result: String) {
      def isSuccess = result == IndexApiResponse.Deleted.toString
    }

    object IndexApiResponse extends Enumeration {
      val Created = Value("created")
      val Deleted = Value("deleted")
    }

    case class IndexAlreadyExistsException(message: String) extends Exception(message)

    case class ElasticErrorResponse(error: JValue, status: Int) extends Exception(s"ElasticsearchError(status=$status): ${error.toString}")

  }

}
