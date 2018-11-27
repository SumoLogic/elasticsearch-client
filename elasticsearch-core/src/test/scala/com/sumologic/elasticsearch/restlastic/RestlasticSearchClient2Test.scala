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

import org.scalatest._

class RestlasticSearchClient2Test extends WordSpec with Matchers with BeforeAndAfterAll
    with ElasticsearchIntegrationTest with OneInstancePerTest with RestlasticSearchClientTest {

  override val restClient = RestlasticSearchClient2Test.restClient

  "RestlasticSearchClient2" should {
    behave like restlasticClient(restClient, IndexName, createIndex(), StringType)
  }
}

object RestlasticSearchClient2Test {
  val restClient = {
    val endpointProvider = new EndpointProvider {
      override def endpoint: Endpoint = Endpoint("127.0.0.1", 9200)
      override def ready: Boolean = true
    }
    new RestlasticSearchClient2(endpointProvider)
  }
}

