package com.sumologic.elasticsearch.akkahelpers

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes
import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.SearchResponse
import com.sumologic.elasticsearch.restlastic.dsl.Dsl._
import com.sumologic.elasticsearch.restlastic.ScrollClient

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class ScanAndScrollSourceStage(name: String = "UnnamedScanAndScrollSourceStage",
                               index: Index,
                               tpe: Type,
                               query: QueryRoot,
                               scrollSource: ScrollClient,
                               sizeOpt: Option[Int],
                               timeout: Duration = 10.seconds)
  extends GraphStage[SourceShape[SearchResponse]] {

  val outlet: Outlet[SearchResponse] = Outlet[SearchResponse](s"$name.out")

  override val shape: SourceShape[SearchResponse] = SourceShape.of(outlet)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with OutHandler {

      private var resultFuture: Future[(ReturnTypes.ScrollId, SearchResponse)] = _

      // TODO: Add some failure management that prints log messages, or throws sensible exceptions
      private def next(): SearchResponse = {
        val (id, data)= Await.result(resultFuture, timeout)
        resultFuture = scrollSource.scroll(id)
        data
      }

      override def preStart(): Unit = {
        resultFuture = scrollSource.startScrollRequest(index, tpe, query, sizeOpt = sizeOpt)
      }

      override def onPull(): Unit = {
        val elem = next()

        if (elem.length == 0) completeStage()

        if (isAvailable(outlet)) {
          push(outlet, elem)
        }
      }

      setHandler(outlet, this)
    }

}
