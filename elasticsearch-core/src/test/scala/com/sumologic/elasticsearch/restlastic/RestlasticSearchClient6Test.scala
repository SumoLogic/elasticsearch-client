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
import akka.http.scaladsl.model.HttpMethods
import akka.util.Timeout
import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.{Bucket, BucketAggregationResultBody, RawJsonResponse}
import com.sumologic.elasticsearch.restlastic.dsl.Dsl._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.time.{Millis, Span}
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class RestlasticSearchClient6Test extends WordSpec with Matchers
    with ElasticsearchIntegrationTest with OneInstancePerTest with RestlasticSearchClientTest {

  override val restClient = RestlasticSearchClient6Test.restClient

  val basicKeywordFieldMapping = BasicFieldMapping(KeywordType, None, None)

  val indices = createIndices(3)
  val index = indices.apply(0)
  val tpe = Type("foo")

  "RestlasticSearchClient6" should {
    behave like restlasticClient(
      restClient,
      IndexName,
      indices,
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
      val termsAggr = TermsAggregation("f1", Some("aggr.*"), None, Some(5), Some(5), Some("map"))
      val aggrQuery = AggregationQuery(filteredQuery, termsAggr, Some(1000))

      val expected = BucketAggregationResultBody(0, 0, List(Bucket("aggr1", 1), Bucket("aggr3", 1)))

      val aggrQueryFuture = restClient.bucketAggregation(index, tpe, aggrQuery)
      aggrQueryFuture.futureValue should be(expected)
    }

    "Support creating data pre-loading indices" in {
      val index = dsl.Dsl.Index(s"$IndexName-preloading")
      val indexSetting = IndexSetting(
        numberOfShards = 1,
        numberOfReplicas = 1,
        analyzerMapping = Analyzers(AnalyzerArray(Analyzer(Name("not_analyzed"), Keyword)), FilterArray()),
        preload = Seq("dvd", "nvm"))
      val indexFut = restClient.createIndex(index, Some(indexSetting))
      indexFut.futureValue
      restClient.runRawEsRequest(
        op = "",
        endpoint = s"/${index.name}/_settings/index.store.preload",
        method = HttpMethods.GET).futureValue should be(
        RawJsonResponse(s"""{"${index.name}":{"settings":{"index":{"store":{"preload":["dvd","nvm"]}}}}}""")
      )
    }

    "Support deleting by a query without waiting for the completion (async)" in {
      implicit val formats = org.json4s.DefaultFormats
      val docsCount = 10011
      val documents = (1 to docsCount).map(i => Document(s"doc$i", Map("text7" -> "here7")))
      restClient.bulkIndex(index, tpe, documents).futureValue
      refresh()

      val count = Await.result(restClient.count(index, tpe, new QueryRoot(MatchAll)), 10.seconds)
      count should be(docsCount)

      val termQuery = TermQuery("text7", "here7")

      val delFut = restClient.deleteByQuery(index, tpe, new QueryRoot(termQuery),
        waitForCompletion = false,
        proceedOnConflicts = true,
        refreshAfterDeletion = true,
        useAutoSlices = true)

      val delResult = delFut.futureValue
      parse(delResult.jsonStr).extract[Map[String, String]].keySet should be (Set("task"))
    }

    "Support deleting by a query with refresh after deletion and slices" in {
      val docsCount = 10011
      val documents = (1 to docsCount).map(i => Document(s"doc$i", Map("text7" -> "here7")))
      val bulkInsertResult = restClient.bulkIndex(index, tpe,documents)
      Await.result(bulkInsertResult, 20.seconds)
      refresh()

      val count = Await.result(restClient.count(index, tpe, new QueryRoot(MatchAll)), 10.seconds)
      count should be(docsCount)

      val termQuery = TermQuery("text7", "here7")

      val delFut = restClient.deleteByQuery(index, tpe, new QueryRoot(termQuery),
        waitForCompletion = true,
        proceedOnConflicts = true,
        refreshAfterDeletion = true,
        useAutoSlices = true)
      Await.result(delFut, 20.seconds)
      // don't need to refresh the index

      val count1 = Await.result(restClient.count(index, tpe, new QueryRoot(termQuery)), 10.seconds)
      count1 should be(0)
    }

    "Add script and check if it exists" in {
      val scriptId = "poof"
      val scriptSource = "return doc[params.hello];"
      val scriptLang = "painless"

      val response = restClient.addScript(scriptId, ScriptSource(scriptLang, scriptSource))
      response.futureValue.acknowledged should be(true)
      val script = restClient.getScript(scriptId)
      val scriptFromES = script.futureValue
      scriptFromES._id should be(scriptId)
      scriptFromES.found should be(true)
      scriptFromES.script should not be(null)
      scriptFromES.script.isDefined should be(true)
      scriptFromES.script.get.lang should be(scriptLang)
      scriptFromES.script.get.source should be(scriptSource)
    }

    "Not find a non-existing script" in {
      val script = restClient.getScript("poof2")
      val scriptFromES = script.futureValue
      scriptFromES._id should be("poof2")
      scriptFromES.found should be(false)
      scriptFromES.script.isEmpty should be(true)
    }

    "Remove an existing script" in {
      val response = restClient.addScript("golden_retriever", ScriptSource("painless", "doc['f1'];"))
      response.futureValue.acknowledged should be(true)
      val removeResponse = restClient.deleteScript("golden_retriever")
      removeResponse.futureValue should be(true)
    }

    "Not remove a non-existing script" in {
      val removeResponse = restClient.deleteScript("bulldog")
      removeResponse.futureValue should be(false)
    }

    "Overwrite an existing script" in {
      val scriptId = "poof"
      val scriptSource = "return doc[params.hello];"
      val scriptLang = "painless"

      val response = restClient.addScript(scriptId, ScriptSource(scriptLang, scriptSource))
      response.futureValue.acknowledged should be(true)
      val responseOverwrite = restClient.addScript(scriptId, ScriptSource(scriptLang, "; " + scriptSource))
      responseOverwrite.futureValue.acknowledged should be(true)
    }

    "Support cardinality aggregations with scripts" in {
      val metadataMapping = Mapping(tpe, IndexMapping(Map("f1" -> basicKeywordFieldMapping)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }

      val docs = Seq(
        Document("cardinalityAggrDoc1", Map("f1" -> "cardinality1")),
        Document("cardinalityAggrDoc2", Map("f1" -> "cardinality1")),
        Document("cardinalityAggrDoc3", Map("f1" -> "cardinality2")),
        Document("cardinalityAggrDoc4", Map("f1" -> "cardinality2")),
        Document("cardinalityAggrDoc5", Map("f1" -> "cardinality2")),
        Document("cardinalityAggrDoc6", Map("f1" -> "cardinality3")),
        Document("cardinalityAggrDoc7", Map("f1" -> "cardinality4")),
        Document("cardinalityAggrDoc8", Map("f1" -> "cardinality4"))
      )
      val bulkIndexFuture = restClient.bulkIndex(index, tpe, docs)
      whenReady(bulkIndexFuture) { _ => refresh() }

      // Inline script
      val cardinalityAggregationSourcedScript =
        CardinalityAggregation(SourcedScriptCardinalityAggregation("return doc['f1'];"))
      val aggrQuerySourcedScript = AggregationQuery(MatchAll, cardinalityAggregationSourcedScript, Some(1000))

      val aggrQueryFutureSourcedScript = restClient.cardinalityAggregation(index, tpe, aggrQuerySourcedScript)
      aggrQueryFutureSourcedScript.futureValue.value should be(4)

      // Stored script
      val response = restClient.addScript("poofpoof", ScriptSource("painless", "return doc['f1'].value;"))
      response.futureValue.acknowledged should be(true)
      val cardinalityAggregationStoredScript =
        CardinalityAggregation(StoredScriptCardinalityAggregation("poofpoof"))
      val aggrQueryStoredScript =
        AggregationQuery(MatchAll, cardinalityAggregationStoredScript, Some(1000))

      val aggrQueryFutStoredScript = restClient.cardinalityAggregation(index, tpe, aggrQueryStoredScript)
      aggrQueryFutStoredScript.futureValue.value should be(4)
    }


    "Support search after" in {
      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("f1" -> basicKeywordFieldMapping, "f2" -> basicNumericFieldMapping, "f3" -> basicKeywordFieldMapping)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }

      // https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#search-after
      val searchAfterDoc1 = Document("searchAfterDoc1", Map("f1" -> "ab", "f2" -> 11, "f3" -> "searchAfter"))
      val searchAfterDoc2 = Document("searchAfterDoc2", Map("f1" -> "aa", "f2" -> 12, "f3" -> "searchAfter"))
      val searchAfterDoc3 = Document("searchAfterDoc3", Map("f1" -> "ab", "f2" -> 12, "f3" -> "searchAfter"))
      val searchAfterFuture = restClient.bulkIndex(index, tpe, Seq(searchAfterDoc1, searchAfterDoc2, searchAfterDoc3))
      whenReady(searchAfterFuture) { ok => refresh() }
      val searchAfterQueryFuture1 = restClient.query(index, tpe, new QueryRoot(
        query = MatchQuery("f3", "searchAfter"),
        fromOpt = None,
        sizeOpt = Some(1),
        sortOpt = Some(Seq(SimpleSort("f1", AscSortOrder), SimpleSort("f2", DescSortOrder))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None,
        searchAfterOpt = None)
      )
      searchAfterQueryFuture1.futureValue.sourceAsMap should be(Seq(
        Map("f1" -> "aa", "f2" -> 12, "f3" -> "searchAfter")
      ))

      val searchAfterQueryFuture2 = restClient.query(index, tpe, new QueryRoot(
        query = MatchQuery("f3", "searchAfter"),
        fromOpt = None,
        sizeOpt = Some(1),
        sortOpt = Some(Seq(SimpleSort("f1", AscSortOrder), SimpleSort("f2", DescSortOrder))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None,
        searchAfterOpt = Some(Seq("aa", 12)))
      )
      searchAfterQueryFuture2.futureValue.sourceAsMap should be(Seq(
        Map("f1" -> "ab", "f2" -> 12, "f3" -> "searchAfter")
      ))

      val searchAfterQueryFuture3 = restClient.query(index, tpe, new QueryRoot(
        query = MatchQuery("f3", "searchAfter"),
        fromOpt = None,
        sizeOpt = Some(1),
        sortOpt = Some(Seq(SimpleSort("f1", AscSortOrder), SimpleSort("f2", DescSortOrder))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None,
        searchAfterOpt = Some(Seq("ab", 12)))
      )
      searchAfterQueryFuture3.futureValue.sourceAsMap should be(Seq(
        Map("f1" -> "ab", "f2" -> 11, "f3" -> "searchAfter")
      ))

      val searchAfterQueryFuture4 = restClient.query(index, tpe, new QueryRoot(
        query = MatchQuery("f3", "searchAfter"),
        fromOpt = None,
        sizeOpt = Some(1),
        sortOpt = Some(Seq(SimpleSort("f1", AscSortOrder), SimpleSort("f2", DescSortOrder))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None,
        searchAfterOpt = Some(Seq("ab", 11)))
      )
      searchAfterQueryFuture4.futureValue.sourceAsMap should be(empty)
    }

  }

  private def refresh(): Unit = {
    implicit val patienceConfig = PatienceConfig(scaled(Span(1500, Millis)), scaled(Span(15, Millis)))
    restClient.refresh(index).futureValue
  }
}

object RestlasticSearchClient6Test {
  val restClient = {
    val endpointProvider = new EndpointProvider {
      override def endpoint: Endpoint = Endpoint("127.0.0.1", 9800)
      override def ready: Boolean = true
    }
    new RestlasticSearchClient6(endpointProvider)(timeout = Timeout(30 seconds))
  }
}

