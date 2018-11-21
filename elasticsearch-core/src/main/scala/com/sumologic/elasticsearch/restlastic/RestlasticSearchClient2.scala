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

class RestlasticSearchClient2(endpointProvider: EndpointProvider, signer: Option[RequestSigner] = None,
                              override val indexExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global,
                              searchExecutionCtx: ExecutionContext = ExecutionContext.Implicits.global)
                             (implicit val system: ActorSystem = ActorSystem(), val timeout: Timeout = Timeout(30.seconds))
  extends RestlasticSearchClient {

  private implicit val formats = org.json4s.DefaultFormats
  private val logger = LoggerFactory.getLogger(RestlasticSearchClient.getClass)
  import Dsl._
  import RestlasticSearchClient.ReturnTypes._

  def ready: Boolean = endpointProvider.ready
  def query(index: Index, tpe: Type, query: RootObject, rawJsonStr: Boolean = true, uriQuery: UriQuery = UriQuery.Empty): Future[SearchResponse] = {
    implicit val ec = searchExecutionCtx
    runEsCommand(query, s"/${index.name}/${tpe.name}/_search", query=uriQuery).map { rawJson =>
      val jsonStr = if(rawJsonStr) rawJson.jsonStr else ""
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
    runEsCommand(CreateIndex(settings), s"/${index.name}").recover {
      case ElasticErrorResponse(message, status) if message.toString contains "index_already_exists_exception" =>
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
              acc ++ items.map(item => Dsl.Index(item._id) -> DeleteResponse(if (item.success) "deleted" else "error")),
              (scrollId) => scroll(scrollId))
          }
      }
    }
  }

  // Scroll requests have optimizations that make them faster when the sort order is _doc.
  // Put sort by _doc in query as described in the the following document
  // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
  def startScrollRequest(index: Index, tpe: Type, query: QueryRoot, resultWindowOpt: Option[String] = None, fromOpt: Option[Int] = None, sizeOpt: Option[Int] = None): Future[(ScrollId, SearchResponse)] = {
    implicit val ec = searchExecutionCtx
    val params = Map("scroll" -> resultWindowOpt.getOrElse(defaultResultWindow)) ++ fromOpt.map("from" -> _.toString) ++ sizeOpt.map("size" -> _.toString)
    runEsCommand(query, s"/${index.name}/${tpe.name}/_search", query = UriQuery(params)).map { resp =>
      val sr = resp.mappedTo[SearchResponseWithScrollId]2
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
        entity = HttpEntity(op))
      signer.map(_.withAuthHeader(unauthed)).getOrElse(unauthed)
    }

    logger.debug(f"Got Rs request: $request (op was $op)")


    val responseFuture: Future[HttpResponse] = (IO(Http) ? request)(timeout).mapTo[HttpResponse]

    responseFuture.map { response =>
      logger.debug(f"Got Es response: $response")
      if (response.status.isFailure) {
        logger.warn(s"Failure response: ${response.entity.asString.take(500)}")
        logger.warn(s"Failing request: ${op.take(5000)}")
        throw ElasticErrorResponse(JString(response.entity.asString), response.status.intValue)
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
