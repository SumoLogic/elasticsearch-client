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
import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.{Bucket, BucketAggregationResultBody}
import com.sumologic.elasticsearch.restlastic.dsl.Dsl._
import org.scalatest._
import org.scalatest.time.{Millis, Span}

class RestlasticSearchClient6Test extends WordSpec with Matchers with BeforeAndAfterAll
    with ElasticsearchIntegrationTest with OneInstancePerTest with RestlasticSearchClientTest {

  override val restClient = RestlasticSearchClient6Test.restClient

  val basicKeywordFieldMapping = BasicFieldMapping(KeywordType, None, None)

  val index = createIndex()

  "RestlasticSearchClient6" should {
    behave like restlasticClient(
      restClient,
      IndexName,
      index,
      TextType,
      basicKeywordFieldMapping)


    "Support keyword normalization" in {
      val tpe = Type("wombat")
      val lcName = Some(Name("keyword_lowercase"))

      val metadataMapping = Mapping(tpe, IndexMapping(
        Map(
          "f1" -> basicKeywordFieldMapping.copy(normalizer = Some(Name("lowercase"))),
          "f2" -> basicNumericFieldMapping,
          "text" -> BasicFieldMapping(TextType, None, lcName, ignoreAbove = None, lcName))))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }

      val aggrDoc1 = Document("aggrDoc1", Map("f1" -> "aGgr1", "f2" -> 1, "text" -> "text1"))
      val aggrDoc2 = Document("aggrDoc2", Map("f1" -> "Aggr2", "f2" -> 2, "text" -> "text2"))
      val aggrDoc3 = Document("aggrDoc3", Map("f1" -> "agGR3", "f2" -> 1, "text" -> "text1"))
      val bulkIndexFuture = restClient.bulkIndex(index, tpe, Seq(aggrDoc1, aggrDoc2, aggrDoc3))
      whenReady(bulkIndexFuture) { _ => refresh() }

      val phasePrefixQuery = PrefixQuery("f1", "aggr")
      val termf = TermFilter("f2", "1")
      val filteredQuery = Bool(List(Must(phasePrefixQuery)), FilteredContext(List(termf)))
      val termsAggr = TermsAggregation("f1", Some("aggr.*"), Some(5), Some(5), Some("map"))
      val aggrQuery = AggregationQuery(filteredQuery, termsAggr, Some(1000))

      val expected = BucketAggregationResultBody(0, 0, List(Bucket("aggr1", 1), Bucket("aggr3", 1)))

      val aggrQueryFuture = restClient.bucketAggregation(index, tpe, aggrQuery)
      aggrQueryFuture.futureValue should be(expected)
    }
  }

  private def refresh(): Unit = {
    restClient.refresh(index).futureValue(PatienceConfig(scaled(Span(1500, Millis)), scaled(Span(15, Millis))))
  }
}

object RestlasticSearchClient6Test {
  val restClient = {
    val endpointProvider = new EndpointProvider {
      override def endpoint: Endpoint = Endpoint("127.0.0.1", 9800)
      override def ready: Boolean = true
    }
    new RestlasticSearchClient6(endpointProvider)
  }
}

