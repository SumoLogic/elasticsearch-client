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

