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

import com.sumologic.elasticsearch.restlastic.dsl.Dsl._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.{Random, Try}

/**
  * Created by Russell Cohen on 11/2/15.
  *
  * A mixin trait for UTs requiring Elasticsearch. The trait will manage a global Elasticsearch instance across all tests.
  *
  * You should use the index `IndexName` provided by the trait as it is guaranteed to be absent when the test starts
  * and will be cleaned up when the test is complete.
  */

trait ElasticsearchIntegrationTest extends BeforeAndAfterAll with ScalaFutures {
  this: Suite =>
  private val indexPrefix = "test-index"

  val endpointProvider = new EndpointProvider {
    override def endpoint: Endpoint = Endpoint("127.0.0.1", 9500)

    override def ready: Boolean = true
  }

  def restClient = new RestlasticSearchClient6(endpointProvider)

  val IndexName = s"test-index-${Random.nextLong()}"
  val index = dsl.Dsl.Index(IndexName)

  override def beforeAll(): Unit = {
    Try(delete(Index(s"$indexPrefix*")))
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    Try(delete(Index(s"$indexPrefix*")))
    super.afterAll()
  }

  def refresh(): Unit = restClient.refresh(index).futureValue(PatienceConfig(scaled(Span(1500, Millis)), scaled(Span(15, Millis))))

  def delete(index: Index) = restClient.deleteIndex(index).futureValue(PatienceConfig(scaled(Span(1500, Millis)), scaled(Span(15, Millis))))
}

