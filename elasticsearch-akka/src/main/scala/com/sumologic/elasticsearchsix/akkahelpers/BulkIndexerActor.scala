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

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.pipe
import com.sumologic.elasticsearchsix.restlastic.RestlasticSearchClient
import com.sumologic.elasticsearchsix.restlastic.RestlasticSearchClient.ReturnTypes.BulkItem
import com.sumologic.elasticsearchsix.restlastic.dsl.Dsl._
import com.sumologic.elasticsearchsix.util.InstrumentationContext
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

case class BulkConfig(flushDuration: () => FiniteDuration, maxDocuments: () => Int)

/**
 * The BulkIndexerActor provides a non-bulk (message-by-message) abstraction on top of the ElasticSearch Bulk API.
 *
 * Every message sent will either return `IndexingComplete` or `IndexingFailure`
 *
 * Note: The BulkIndexerActor does not implement it's own backpressure. The client must account for the number of messages
 * in flight to control memory usage.
 * @param restlasticSearchClient Underlying ElasticsearchClient supporting the bulk API
 * @param bulkConfig Flushing configuration
 */
class BulkIndexerActor(restlasticSearchClient: RestlasticSearchClient, bulkConfig: BulkConfig) extends Actor {
  import BulkIndexerActor._
  implicit val ec = restlasticSearchClient.indexExecutionCtx

  val logger = LoggerFactory.getLogger(BulkIndexerActor.getClass)

  case class OpWithTarget(operation: BulkOperation, target: ActorRef, sessionId: BulkSession)
  var queue = List.empty[OpWithTarget]

  private def resetTimer() = context.system.scheduler.scheduleOnce(bulkConfig.flushDuration(), self, ForceFlush)
  var flushTimer = resetTimer()

  override def receive: Receive = {

    case CreateRequest(sess, index, tpe, doc) =>
      queue = OpWithTarget(BulkOperation(create, Some(index -> tpe), doc), sender(), sess) :: queue
      if (queueFull) flush()

    case IndexRequest(sess, indexName, tpe, doc) =>
      queue = OpWithTarget(BulkOperation(index, Some(indexName -> tpe), doc), sender(), sess) :: queue
      if (queueFull) flush()

    case UpdateRequest(sess, index, tpe, doc, retryOnConflictOpt, upsertOpt, docAsUpsertOpt) =>
      queue = OpWithTarget(BulkOperation(update, Some(index -> tpe), doc, retryOnConflictOpt, upsertOpt, docAsUpsertOpt), sender(), sess) :: queue
      if (queueFull) flush()

    case ForceFlush => flush()

    case IndexingComplete(items) =>
        items.foreach { case (item, target, sess) =>
          target ! DocumentIndexed(sess, item)
        }

    case IndexingFailure(cause, actorsAndSessions) =>
        actorsAndSessions.foreach { case (target, sess) =>
          target ! DocumentNotIndexed(sess, cause)
        }
  }

  private def now = System.currentTimeMillis()
  private def ago(millis: Long) = now - millis

  private def flush(): Unit = {

    if (queue.isEmpty) {
      flushTimer = resetTimer()
      return
    }

    val instrument = new InstrumentationContext
    flushTimer.cancel()
    val messages = queue

    queue = List.empty[OpWithTarget]
    val bulkRequest = instrument.measureAction("createRequest") {
      Bulk(messages.map(_.operation))
    }
    val startTime = now
    val respFuture = instrument.measureAction("sendRequest") {
      val respMb = bulkRequest.toJsonStr.length / 1024.0 / 1024
      logger.info(f"Flushing ${messages.length} messages ($respMb%.2fMB).")
      restlasticSearchClient.bulkIndex(bulkRequest).map { items =>
        if (items.length == messages.length) {
          val zipped = items.zip(messages).map { case (item, optarget) => (item, optarget.target, optarget.sessionId) }
          logger.info(f"Flushed ${messages.length} messages ($respMb%.2fMB). esTimeMs=${ago(startTime)}; ${instrument.measurementsString}")
          IndexingComplete(zipped)
        } else {
          logger.error(s"Number of responses ${items.length} != requests(${messages.length})")
          IndexingFailure(new IllegalStateException("Bad response count"), messages.map(op => op.target -> op.sessionId))
        }
      }.recover { case ex: Throwable =>
        logger.warn(s"Failed to flush ${messages.length} messages. esTimeMs=${ago(startTime)}; ${instrument.measurementsString}", ex)
        IndexingFailure(ex, messages.map(op => op.target -> op.sessionId))
      }
    }

    flushTimer = resetTimer()
    respFuture pipeTo self
  }

  private def queueFull: Boolean = queue.size >= bulkConfig.maxDocuments()
}

object BulkIndexerActor {

  def props(searchClient: RestlasticSearchClient, config: BulkConfig) = Props(new BulkIndexerActor(searchClient, config))

  // Messages
  case class CreateRequest(sessionId: BulkSession, index: Index, tpe: Type, doc: Document)
  case class IndexRequest(sessionId: BulkSession, index: Index, tpe: Type, doc: Document)
  case class UpdateRequest(sessionId: BulkSession, index: Index, tpe: Type, doc: Document, retryOnConflictOpt: Option[Int] = None, upsertOpt: Option[Document] = None, docAsUpsertOpt: Option[Boolean] = None)
  case object ForceFlush

  // Replies
  case class DocumentIndexed(sessionId: BulkSession, doc: BulkItem)
  case class DocumentNotIndexed(sessionId: BulkSession, cause: Throwable)

  case class BulkSession(id: Long)
  case object BulkSession {
    val r = new Random()
    def create() = BulkSession(r.nextLong())
  }

  private case class IndexingComplete(result: Seq[(BulkItem, ActorRef, BulkSession)])
  private case class IndexingFailure(cause: Throwable, replyInfo: Seq[(ActorRef, BulkSession)])
}


