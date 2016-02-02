package com.sumologic.elasticsearch.akkahelpers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes._
import com.sumologic.elasticsearch.restlastic.ScrollClient
import com.sumologic.elasticsearch.restlastic.dsl.Dsl
import com.sumologic.elasticsearch.restlastic.dsl.Dsl._
import org.json4s.Extraction._
import org.json4s._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Future

class ScanAndScrollSourceTest extends WordSpec with Matchers with ScalaFutures {
  val resultMaps: List[Map[String, AnyRef]] = List(Map("a" -> "1"), Map("a" -> "2"), Map("a" -> "3"))
  implicit val formats = org.json4s.DefaultFormats
  implicit val system = ActorSystem("test")
  implicit val materializer = ActorMaterializer()

  def searchResponseFromMap(map: Map[String, AnyRef]) = {
    val raw = RawSearchResponse(Hits(List(ElasticJsonDocument("index", "type", "id", 0.1f, decompose(map).asInstanceOf[JObject]))))
    SearchResponse(raw, "{}")
  }

  "ScanAndScrollSource" should {
    val index = Index("index")
    val tpe = Type("tpe")
    val queryRoot = QueryRoot(MatchAll)

    "Read to the end of a source" in {
      val searchResponses = resultMaps.map(searchResponseFromMap)
      val client = new MockScrollClient(searchResponses)
      val source = Source.actorPublisher[SearchResponse](ScanAndScrollSource.props(index, tpe, queryRoot, client))
      val fut = source
        .map(_.sourceAsMap)
        .grouped(10)
        .runWith(Sink.head)
      whenReady(fut) { resp =>
        resp.flatten should be(resultMaps)
      }
    }
  }
}


class MockScrollClient(results: List[SearchResponse]) extends ScrollClient {
  var id = 1
  var started = false
  var resultsQueue = results
  override def startScrollRequest(index: Dsl.Index, tpe: Dsl.Type, query: Dsl.QueryRoot,
                                  resultWindow: String): Future[ScrollId] = {
    if (!started) {
      started = true
      Future.successful(ScrollId(id.toString))
    } else {
      Future.failed(new RuntimeException("Scroll already started"))
    }
  }

  override def scroll(scrollId: ScrollId, resultWindow: String): Future[(ScrollId, SearchResponse)] = {
    if (scrollId.id.toInt == id) {
      id += 1
      resultsQueue match {
        case head :: rest =>
          resultsQueue = rest
          Future.successful((ScrollId(id.toString), head))
        case Nil =>
          Future.successful((ScrollId(id.toString), SearchResponse.empty))
      }
    } else {
      Future.failed(new RuntimeException("Invalid id"))
    }

  }
}
