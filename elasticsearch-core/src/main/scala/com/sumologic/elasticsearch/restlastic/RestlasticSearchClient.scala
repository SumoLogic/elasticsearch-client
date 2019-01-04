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

import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.{ScrollId, SearchResponse}
import com.sumologic.elasticsearch.restlastic.dsl.{Dsl, EsVersion}
import org.json4s.FieldSerializer.{renameFrom, renameTo}
import org.json4s._
import org.json4s.native.JsonMethods._
import spray.http.HttpMethods._
import spray.http.Uri.{Query => UriQuery}
import spray.http._

import scala.concurrent.{ExecutionContext, Future}

trait ScrollClient {

  import Dsl._

  val defaultResultWindow = "1m"
  val indexExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global

  // Scroll requests have optimizations that make them faster when the sort order is _doc.
  // Put sort by _doc in query as described in the the following document
  // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
  def startScrollRequest(index: Index,
                         tpe: Type,
                         query: QueryRoot,
                         resultWindowOpt: Option[String] = None,
                         fromOpt: Option[Int] = None,
                         sizeOpt: Option[Int] = None,
                         preference: Option[String] = None): Future[(ScrollId, SearchResponse)]

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

trait RestlasticSearchClient extends ScrollClient {

  import Dsl._
  import RestlasticSearchClient.ReturnTypes._

  def ready: Boolean

  def query(index: Index, tpe: Type, query: RootObject, rawJsonStr: Boolean = true, uriQuery: UriQuery = UriQuery.Empty): Future[SearchResponse]

  def bucketNestedAggregation(index: Index, tpe: Type, query: AggregationQuery): Future[BucketNested]

  def bucketAggregation(index: Index, tpe: Type, query: AggregationQuery): Future[BucketAggregationResultBody]

  def suggest(index: Index, tpe: Type, query: SuggestRoot): Future[Map[String,List[String]]]

  def count(index: Index, tpe: Type, query: QueryRoot): Future[Int]

  def index(index: Index, tpe: Type, doc: Document): Future[IndexResponse]

  def deleteById(index: Index, tpe: Type, id: String): Future[DeleteResponse]

  def documentExistsById(index: Index, tpe: Type, id: String): Future[Boolean]

  def bulkIndex(bulk: Bulk): Future[Seq[BulkItem]]

  def bulkIndex(index: Index, tpe: Type, documents: Seq[Document]): Future[Seq[BulkItem]]

  // retryOnConflictOpt specifies how many times to retry before throwing version conflict exception.
  // https://www.elastic.co/guide/en/elasticsearch/reference/2.3/docs-update.html#_parameters_2
  def bulkUpdate(index: Index, tpe: Type, documents: Seq[Document], retryOnConflictOpt: Option[Int] = None): Future[Seq[BulkItem]]

  def bulkDelete(index: Index, tpe: Type, documents: Seq[Document]): Future[Seq[BulkItem]]

  def putMapping(index: Index, tpe: Type, mapping: Mapping): Future[RawJsonResponse]

  def getMapping(index: Index, tpe: Type): Future[RawJsonResponse]

  def createIndex(index: Index, settings: Option[IndexSetting] = None): Future[RawJsonResponse]

  def deleteIndex(index: Index): Future[RawJsonResponse]

  @deprecated("When plugin is not enabled this function doesn't handle pagination, so it deletes only first page of query results. Replaced by deleteDocuments.")
  def deleteDocument(index: Index, tpe: Type, deleteQuery: QueryRoot, pluginEnabled: Boolean = false): Future[RawJsonResponse]

  def deleteDocuments(index: Index, tpe: Type, deleteQuery: QueryRoot, pluginEnabled: Boolean = false): Future[Map[Index, DeleteResponse]]

  def scroll(scrollId: ScrollId, resultWindowOpt: Option[String] = None): Future[(ScrollId, SearchResponse)]

  def flush(index: Index): Future[RawJsonResponse]

  def refresh(index: Index): Future[RawJsonResponse]

  def runRawEsRequest(op: String,
                      endpoint: String,
                      method: HttpMethod = POST,
                      query: UriQuery = UriQuery.Empty)
                     (implicit ec: ExecutionContext = ExecutionContext.Implicits.global): Future[RawJsonResponse]

  def version: EsVersion
}

object RestlasticSearchClient {

  object ReturnTypes {

    case class ScrollId(id: String)

    case class BulkIndexResponse(items: List[Map[String, BulkItem]])

    case class BulkIndexError(reason: String)

    case class BulkItem(_index: String, _type: String, _id: String, status: Int, error: Option[BulkIndexError]) {
      def created: Boolean = status > 200 && status < 299 && !alreadyExists

      def alreadyExists: Boolean = error.exists(_.reason.contains("document already exists"))

      def success: Boolean = status >= 200 && status <= 299
    }

    case class SearchResponse(rawSearchResponse: RawSearchResponse, jsonStr: String) {
      def extractSource[T: Manifest]: Seq[T] = {
        rawSearchResponse.extractSource[T]
      }

      def sourceAsMap: Seq[Map[String, Any]] = rawSearchResponse.sourceAsMap

      def highlightAsMaps: Seq[Map[String, Any]] = rawSearchResponse.highlightAsMaps

      def length: Int = rawSearchResponse.hits.hits.length
    }

    object SearchResponse {
      val empty = SearchResponse(RawSearchResponse(Hits(List())), "{}")
    }

    case class CountResponse(count: Int)

    case class SearchResponseWithScrollId(_scroll_id: String, hits: Hits)

    case class RawSearchResponse(hits: Hits) {
      private implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      def extractSource[T: Manifest]: Seq[T] = {
        hits.hits.map(_._source.extract[T])
      }

      def sourceAsMap: Seq[Map[String, Any]] = hits.hits.map(_._source.values)

      def highlightAsMaps: Seq[Map[String, Any]] = hits.hits.flatMap(_.highlight.map(_.values))

      def innerHits: List[JObject] = hits.hits.flatMap(_.inner_hits)
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
                                   highlight: Option[JObject],
                                   inner_hits: Option[JObject])

    case class RawJsonResponse(jsonStr: String) {
      private val SuggesionOptionDeserializer = FieldSerializer[SuggestOption](
        renameTo("_score", "score"),
        renameFrom("score", "_score"))

      private implicit val formats: Formats = org.json4s.DefaultFormats + SuggesionOptionDeserializer

      def mappedTo[T: Manifest]: T = {
        val jsonTree = parse(jsonStr)
        jsonTree.extract[T]
      }
    }

    case class SuggestResult(suggest: Map[String, List[Suggestion]]) {
      def suggestions: Map[String, List[String]] = {
        suggest.map{ case (name, suggestions) => name -> suggestions.flatMap(_.options.map(_.text))}
      }
    }

    case class Suggestion(text: String, options: List[SuggestOption])

    case class SuggestOption(text: String, _score: Float)

    case class IndexResponse(result: String) {
      def this(created: Boolean) = this(if (created) IndexApiResponse.Created.toString else "error")

      def isSuccess: Boolean = result == IndexApiResponse.Created.toString
    }

    case class DeleteResponse(result: String) {
      def this(found: Boolean) {
        this(if (found) "deleted" else "not_found")
      }

      def isSuccess: Boolean = result == IndexApiResponse.Deleted.toString
    }

    object IndexApiResponse extends Enumeration {
      val Created: IndexApiResponse.Value = Value("created")
      val Deleted: IndexApiResponse.Value = Value("deleted")
    }

    case class IndexAlreadyExistsException(message: String) extends Exception(message)

    case class ElasticErrorResponse(error: JValue, status: Int) extends Exception(s"ElasticsearchError(status=$status): ${error.toString}")

  }

}
