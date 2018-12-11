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
package com.sumologic.elasticsearchsix.akkahelpers

import akka.actor.FSM.Failure
import akka.actor.{FSM, Props}
import akka.pattern.pipe
import akka.stream.actor.ActorPublisher
import com.sumologic.elasticsearchsix.akkahelpers.ScanAndScrollSource.{ScanData, ScanState}
import com.sumologic.elasticsearchsix.restlastic.RestlasticSearchClient.ReturnTypes.{ScrollId, SearchResponse}
import com.sumologic.elasticsearchsix.restlastic.ScrollClient
import com.sumologic.elasticsearchsix.restlastic.dsl.Dsl._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

/**
 * ScanAndScrollSource wraps Elasticsearch's Scroll API as a akka-streams source. By creating and subscribing to this source,
 * you will get a stream of every message in Elasticsearch matching your query. Internally, the messages are batched. The batch
 * size is configurable by setting a size parameter on the query. Externally, results are streamed message-by-message.
 *
 * The Source will only continue getting more results from Elasticsearch will downstream is consuming. In the future, we may need two enhancements:
 * - Keep alive messages to keep the source alive >1m
 * - Buffering of more messages
 * @param index Index to search
 * @param tpe Type to search
 * @param query Query -- probably want MatchAll. You can also specify that batch size
 * @param scrollSource Raw ES scroll interface
 */

class ScanAndScrollSource(index: Index, tpe: Type,
                          query: QueryRoot,
                          scrollSource: ScrollClient,
                          sizeOpt: Option[Int] = None,
                          executionContext: ExecutionContext = ExecutionContext.Implicits.global)
  extends ActorPublisher[SearchResponse]
  with FSM[ScanState, ScanData] {

  import akka.stream.actor.ActorPublisherMessage

  import ScanAndScrollSource._

  implicit val ec: ExecutionContext = executionContext

  val logger = LoggerFactory.getLogger(ScanAndScrollSource.getClass)
  override def preStart(): Unit = {
    scrollSource.startScrollRequest(index, tpe, query, sizeOpt = sizeOpt).map { case (scrollId, newData) =>
      GotData(scrollId, newData)
    }.recover(recovery).pipeTo(self)
    startWith(Starting, FirstScroll)
  }

  when(Starting) {
    case Event(ActorPublisherMessage.Request(_), _) => stay()
    case Event(ActorPublisherMessage.Cancel, _) => stop()

    case Event(GotData(nextId, data), FirstScroll) =>
      consumeIfNotComplete(nextId, data)
    case Event(ScrollFailure(ex), _) =>
      onError(ScrollFailureException("Failed to start the scroll", ex))
      stop(Failure(ex))
  }

  when(Running) {
    case Event(ActorPublisherMessage.Request(_), WithIdAndData(id, data)) =>
      consumeIfPossible(id, data)

    case Event(ActorPublisherMessage.Request(_), WaitingForDataWithId(id)) =>
      // Nothing to do, just waiting for data
      stay()

    case Event(ActorPublisherMessage.Cancel, _) =>
      // TODO: cancel the scroll
      stop()

    case Event(ScrollFailure(ex), _) =>
      onError(ScrollFailureException("Failure while running the scroll", ex))
      stop(Failure(ex))

    case Event(GotData(nextId, data), WaitingForDataWithId(currentId)) =>
      consumeIfNotComplete(nextId, data)
  }

  whenUnhandled {
    case Event(otherEvent, otherData) =>
      logger.warn(s"Unhandled event: $otherEvent, $otherData")
      stay()

  }

  private def consumeIfNotComplete(nextId: ScrollId, data: SearchResponse) = {
    if (data.length == 0) {
      onComplete()
      stop()
    } else {
      consumeIfPossible(nextId, data)
    }
  }

  private def consumeIfPossible(id: ScrollId, data: SearchResponse) = {
    if (totalDemand > 0) {
      onNext(data)
      requestMore(id)
      goto(Running) using WaitingForDataWithId(id)
    } else {
      goto(Running) using WithIdAndData(id, data)
    }
  }

  private def requestMore(id: ScrollId) = {
    scrollSource.scroll(id).map { case (scrollId, newData) =>
      GotData(scrollId, newData)
    }.recover(recovery).pipeTo(self)
    goto(Running) using WaitingForDataWithId(id)
  }

  private val recovery: PartialFunction[Throwable, ScrollFailure] = {
    case ex => ScrollFailure(ex)
  }
}

object ScanAndScrollSource {
  def props(index: Index,
            tpe: Type,
            query: QueryRoot,
            scrollSource: ScrollClient,
            sizeOpt: Option[Int] = None,
            executionContext: ExecutionContext = ExecutionContext.Implicits.global) = {
    Props(new ScanAndScrollSource(index, tpe, query, scrollSource, sizeOpt, executionContext))
  }

  case class ScrollFailureException(message: String, cause: Throwable) extends Exception(message, cause)

  sealed trait ScanState
  case object Starting extends ScanState
  case object Running extends ScanState

  sealed trait ScanData
  case object FirstScroll extends ScanData
  case class WaitingForDataWithId(scrollId: ScrollId) extends ScanData
  case class WithIdAndData(scrollId: ScrollId, data: SearchResponse) extends ScanData

  case class GotData(nextScrollId: ScrollId, data: SearchResponse)
  case class ScrollFailure(cause: Throwable)

}


