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

import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.{Suggestion => _, _}
import com.sumologic.elasticsearch.restlastic.dsl.{Dsl, V2, V6}
import com.sumologic.elasticsearch.restlastic.dsl.Dsl._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpec}
import spray.http.HttpMethods.GET

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

trait RestlasticSearchClientTest {
  this: WordSpec with ScalaFutures with Matchers =>

  import scala.concurrent.ExecutionContext.Implicits.global

  protected val basicNumericFieldMapping = BasicFieldMapping(IntegerType, None, None, None, None)

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(Span(10, Seconds)), interval = scaled(Span(50, Millis)))

  def restlasticClient(restClient: => RestlasticSearchClient,
                       indexName: String,
                       index: Dsl.Index,
                       textType: FieldType,
                       basicKeywordFieldMapping: BasicFieldMapping): Unit = {
    val keywordType = basicKeywordFieldMapping.tpe
    val analyzerName = Name("keyword_lowercase")
    val basicTextFieldMapping = BasicFieldMapping(textType, None, Some(analyzerName), ignoreAbove = None, Some(analyzerName))
    val tpe = Type("foo")

    def refreshWithClient(): Unit = {
      Await.result(restClient.refresh(index), 2.seconds)
    }

    def refresh(): Unit = {
      restClient.refresh(index).futureValue(PatienceConfig(scaled(Span(1500, Millis)), scaled(Span(15, Millis))))
    }

    def indexDocs(docs: Seq[Document]): Unit = {
      val bulkIndexFuture = restClient.bulkIndex(index, tpe, docs)
      whenReady(bulkIndexFuture) { _ => refresh() }
    }

    "Be able to create an index and setup index setting with keyword & edgengram lowercase analyzer" in {
      val edgeNgram = EdgeNGramFilter(Name(EdgeNGram.rep), 1, 20)
      val edgeNgramLowercaseAnalyzer = Analyzer(Name(s"${EdgeNGram.rep}_lowercase"), Keyword, Lowercase, EdgeNGram)
      val keywordLowercaseAnalyzer = Analyzer(analyzerName, Keyword, Lowercase)
      val analyzers = Analyzers(
        AnalyzerArray(keywordLowercaseAnalyzer, edgeNgramLowercaseAnalyzer), FilterArray(edgeNgram))
      val indexSetting = IndexSetting(12, 1, analyzers, 30)
      val indexFut = restClient.createIndex(Index(s"${indexName}_${EdgeNGram.rep}"), Some(indexSetting))
      whenReady(indexFut) { _ => refreshWithClient() }
    }

    "Be able to create an index with pattern_capture analyzer" in {
      val pattern = PatternFilter(Name(PatternFilter.rep), preserveOriginal = false, Seq("(\\W+)"))
      val analyzer = Analyzer(Name("pattern_lowercase"), Keyword, PatternFilter)
      val analyzers = Analyzers(AnalyzerArray(analyzer), FilterArray(pattern))
      val indexSetting = IndexSetting(12, 1, analyzers, 30)
      val indexFut = restClient.createIndex(Index(s"${indexName}_${EdgeNGram.rep}"), Some(indexSetting))
      whenReady(indexFut) { _ => refreshWithClient() }
    }

    "Be able to setup document mapping" in {
      val basicFieldMapping = BasicFieldMapping(textType, None, Some(analyzerName))
      val metadataMapping = Mapping(tpe, IndexMapping(
        Map(
          "name" -> basicFieldMapping,
          "f1" -> basicFieldMapping,
          "suggest" -> CompletionMapping(Map("f" -> CompletionContext("name")), analyzerName)), Some(false)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }
    }

    "Be able to setup document mapping with ignoreAbove" in {
      val basicFieldMapping = BasicFieldMapping(keywordType, None, None, ignoreAbove = Some(10000), None)
      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("name" -> basicFieldMapping, "f1" -> basicFieldMapping, "suggest" -> CompletionMapping(Map("f" -> CompletionContext("name")), analyzerName))))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }
    }

    "Be able to setup document mapping with field index options" in {
      val basicFieldDocsMapping = BasicFieldMapping(textType, None, Some(analyzerName), None,
        Some(analyzerName), indexOption = Some(DocsIndexOption))
      val basicFieldFreqsMapping = BasicFieldMapping(textType, None, Some(analyzerName), None,
        Some(analyzerName), indexOption = Some(FreqsIndexOption))
      val basicFieldOffsetsMapping = BasicFieldMapping(textType, None, Some(analyzerName), None,
        Some(analyzerName), indexOption = Some(OffsetsIndexOption))

      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("name" -> basicFieldDocsMapping, "f1" -> basicFieldFreqsMapping,
          "f2" -> basicFieldOffsetsMapping, "text" -> basicFieldOffsetsMapping,
          "suggest" -> CompletionMapping(Map("f" -> CompletionContext("name")),
            analyzerName))))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }
      val mappingRes = restClient.getMapping(index, tpe)

      mappingRes.futureValue.jsonStr.toString.contains(s""""f2":{"type":"${textType.rep}","index_options":"offsets","analyzer":"keyword_lowercase"}""") should be(true)
    }

    "Be able to create an index, index a document, and search it" in {
      indexDocs(Seq(Document("doc1", Map("text" -> "here"))))
      val resFut = restClient.query(index, tpe, new QueryRoot(TermQuery("text", "here"), timeoutOpt = Some(5000)))

      whenReady(resFut) { res =>
        res.sourceAsMap.toList should be(List(Map("text" -> "here")))
      }
    }

    "Be able to delete a document that exists" in {
      indexDocs(Seq(Document("doc1", Map("text" -> "here"))))
      val resFut = restClient.query(index, tpe, new QueryRoot(TermQuery("text", "here"), timeoutOpt = Some(5000)))

      val foundDoc: ElasticJsonDocument = whenReady(resFut) { res =>
        res.rawSearchResponse.hits.hits.head
      }

      val delFut = restClient.deleteById(index, tpe, foundDoc._id)

      whenReady(delFut) { res =>
        res.isSuccess should be(true)
      }

      refresh()

      val resFut2 = restClient.query(index, tpe, new QueryRoot(TermQuery("text", "here"), timeoutOpt = Some(5000)))
      whenReady(resFut2) { res =>
        res.rawSearchResponse.hits.hits.size should be(0)
      }
    }

    "Not delete a document that does not exist" in {
      val delFut = restClient.deleteById(index, tpe, "fakeId")

      whenReady(delFut.failed) {
        case e: ElasticErrorResponse => e.status should be(404)
        case other => fail(s"Error expected, got $other")
      }
    }

    "Throw IndexAlreadyExists exception" in {
      val res = for {
        _ <- restClient.createIndex(index)
        _ <- restClient.createIndex(index)
      } yield {
        "created"
      }
      intercept[IndexAlreadyExistsException] {
        Await.result(res, 10.seconds)
      }
    }

    "Support document mapping" in {
      val doc = Document("doc6", Map("f1" -> "f1value", "f2" -> 5))
      val fut = restClient.index(index, tpe, doc)
      whenReady(fut) { _ => refresh() }
      val resFut = restClient.query(index, tpe, new QueryRoot(TermQuery("f1", "f1value")))
      whenReady(resFut) { resp =>
        resp.extractSource[DocType].head should be(DocType("f1value", 5))
      }
    }

    "Support bulk indexing and deletion" in {

      val doc3 = Document("doc3", Map("text" -> "here"))
      val doc4 = Document("doc4", Map("text" -> "here"))
      val doc5 = Document("doc5", Map("text" -> "nowhere"))

      // doc3 is inserted twice, so when it is inserted in bulk, it should have already been created
      val fut = for {
        _ <- restClient.index(index, tpe, doc3)
        bulk <- restClient.bulkIndex(index, tpe, Seq(doc3, doc4, doc5))
      } yield {
        bulk
      }
      whenReady(fut) { resp =>
        resp.length should be(3)
        resp.head.created should be(false)
        resp.head.alreadyExists should be(true)
        resp(1).created should be(true)
        resp(2).created should be(true)
      }

      refresh()
      val resFut = restClient.query(index, tpe, new QueryRoot(TermQuery("text", "here")))
      whenReady(resFut) { res =>
        res.jsonStr should include("doc3")
        res.jsonStr should include("doc4")
        res.jsonStr should not include "doc5"
      }

      val delFut = restClient.bulkDelete(index, tpe, Seq(doc3, doc4, doc5))
      whenReady(delFut) { resp =>
        resp.length should be(3)
        resp.head.success should be(true)
        resp(1).success should be(true)
        resp(2).success should be(true)
      }

      refresh()
      val resFut2 = restClient.query(index, tpe, new QueryRoot(TermQuery("text", "here")))
      whenReady(resFut2) { res =>
        res.jsonStr should not include "doc3"
        res.jsonStr should not include "doc4"
        res.jsonStr should not include "doc5"
      }
    }

    "Support bulk updates with retry on conflict" in {

      val doc = Document("doc", Map("text" -> "retry on version conflict"))
      val indexFuture = restClient.index(index, tpe, doc)
      indexFuture.futureValue.isSuccess should be(true)

      val docUpdate1 = Document("doc", Map("text" -> "retry on version conflict 1"))
      val docUpdate2 = Document("doc", Map("text" -> "retry on version conflict 2"))
      val docUpdate3 = Document("doc", Map("text" -> "retry on version conflict 3"))

      val bulkUpdateFutureWithRetry = (1 to 5).map { _ =>
        restClient.bulkUpdate(index, tpe, Seq(docUpdate1, docUpdate2, docUpdate3), retryOnConflictOpt = Some(100))
      }
      Future.sequence(bulkUpdateFutureWithRetry).futureValue.toString().contains("version conflict") should be(false)
    }

    "Support scroll requests" in {
      val docFutures = (1 to 15).map { n =>
        Document(s"doc-$n", Map("ct" -> "ct", "id" -> n))
      }.map { doc =>
        restClient.index(index, tpe, doc)
      }
      whenReady(Future.sequence(docFutures)) { _ =>
        refresh()
      }
      val query = new QueryRoot(MatchAll, sortOpt = Some(Seq(SimpleSort("id", AscSortOrder))))
      val fut = restClient.startScrollRequest(index, tpe, query, sizeOpt = Some(5))
      val scrollId = whenReady(fut) { case (id, data) =>
        data.sourceAsMap.flatMap(_.values).filter(_ != "ct") should be(List(1, 2, 3, 4, 5))
        id
      }
      whenReady(restClient.scroll(scrollId)) { case (id, data) =>
        data.sourceAsMap.flatMap(_.values).filter(_ != "ct") should be(List(6, 7, 8, 9, 10))
      }
      whenReady(restClient.scroll(scrollId)) { case (id, data) =>
        data.sourceAsMap.flatMap(_.values).filter(_ != "ct") should be(List(11, 12, 13, 14, 15))
      }
    }

    "Support the count API" in {
      val docFutures = (1 to 10).map { n =>
        Document(s"doc-$n", Map("ct" -> "ct", "id" -> n))
      }.map { doc =>
        restClient.index(index, tpe, doc)
      }

      val docs = Future.sequence(docFutures)
      whenReady(docs) { _ =>
        refresh()
      }
      val ctFut = restClient.count(index, tpe, new QueryRoot(TermQuery("ct", "ct")))
      whenReady(ctFut) { ct =>
        ct should be(10)
      }
    }

    "Support raw requests" in {
      val future = restClient.runRawEsRequest(op = "", endpoint = "/_cat/indices", GET)
      whenReady(future) { res =>
        res.jsonStr should include(indexName)
      }
    }

    "Return error on failed raw requests" in {
      val future = restClient.runRawEsRequest(op = "", endpoint = "/does/not/exist", GET)
      whenReady(future.failed) { e =>
        e shouldBe a[ElasticErrorResponse]
        val elasticErrorResponse = e.asInstanceOf[ElasticErrorResponse]
        elasticErrorResponse.status should be(404)
      }
    }

    "Be able to parse error in response as a json object" in {
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
      val errorDocumentMissing =
        """{"error":{"root_cause":[{"type":"document_missing_exception",
          |"reason":"[engine_product][37035513]: document missing","shard":"1","index":"product"}],
          |"type":"document_missing_exception",
          |"reason":"[engine_product][37035513]: document missing",
          |"shard":"1",
          |"index":"product"},
          |"status":404} """.stripMargin
      val jsonTree = parse(errorDocumentMissing)
      val errorMessage = jsonTree.extract[ElasticErrorResponse]
      errorMessage.error should be(jsonTree \\ "error")
    }

    "Support delete documents" in {
      val ir = restClient.index(index, tpe, Document("doc7", Map("text7" -> "here7")))
      whenReady(ir) { ir =>
        ir.isSuccess should be(true)
      }
      refresh()
      val resFut = restClient.query(index, tpe, new QueryRoot(TermQuery("text7", "here7")))
      whenReady(resFut) { res =>
        res.sourceAsMap.toList should be(List(Map("text7" -> "here7")))
      }
      val delFut = restClient.deleteDocument(index, tpe, new QueryRoot(TermQuery("text7", "here7")))
      Await.result(delFut, 10.seconds)
      refresh()
      val resFut1 = restClient.query(index, tpe, new QueryRoot(TermQuery("text7", "here7")))
      whenReady(resFut1) { res =>
        res.sourceAsMap.toList should be(List())
      }
    }

    "Support bulk update document when document does not exist" in {
      val doc1 = Document("bulk_doc1", Map("text" -> "here"))
      val doc2 = Document("bulk_doc2", Map("text" -> "here"))
      val doc3 = Document("bulk_doc3", Map("text" -> "here"))

      val fut = for {
        bulk <- restClient.bulkUpdate(index, tpe, Seq(doc1, doc2, doc3))
      } yield {
        bulk
      }

      whenReady(fut) { resp =>
        resp.length should be(3)
        resp.head.created should be(true)
        resp(1).created should be(true)
        resp(2).created should be(true)
      }

      refresh()
      val resFut = restClient.query(index, tpe, new QueryRoot(TermQuery("text", "here")))
      whenReady(resFut) { res =>
        res.jsonStr should include("bulk_doc1")
        res.jsonStr should include("bulk_doc2")
        res.jsonStr should include("bulk_doc3")
      }
    }

    "Support bulk update document when document exists with different content" in {
      val doc1 = Document("bulk_doc1", Map("text" -> "updated"))
      val doc2 = Document("bulk_doc2", Map("text" -> "updated"))
      val doc3 = Document("bulk_doc3", Map("text" -> "updated"))

      val docs = Seq(doc1, doc2, doc3)
      indexDocs(docs)

      val fut = restClient.bulkUpdate(index, tpe, docs)

      whenReady(fut) { resp =>
        resp.length should be(3)
        resp.head.created should be(false)
        resp(1).created should be(false)
        resp(2).created should be(false)
      }

      refresh()
      val resFut = restClient.query(index, tpe, new QueryRoot(TermQuery("text", "updated")))
      whenReady(resFut) { res =>
        res.jsonStr should include("bulk_doc1")
        res.jsonStr should include("bulk_doc2")
        res.jsonStr should include("bulk_doc3")
      }
    }

    "Support bulk partial document updates with whole document upserts" in {
      val docOrigin1 = Document("bulk_upsert_doc1", Map("text" -> "original", "foo" -> "bar"))
      val docs = Seq(docOrigin1)
      indexDocs(docs)

      val docUpdate1 = Document("bulk_upsert_doc1", Map("text" -> "update"))
      val docUpdate2 = Document("bulk_upsert_doc2", Map("text" -> "update"))

      val docUpsert1 = Document("bulk_upsert_doc1", Map("text" -> "update", "notfoo" -> "notbar"))
      val docUpsert2 = Document("bulk_upsert_doc2", Map("text" -> "update", "foo" -> "bar"))

      val bulkOperation1 = BulkOperation(update, Some(index, tpe), docUpdate1, retryOnConflictOpt = Some(5), upsertOpt = Some(docUpsert1))
      val bulkOperation2 = BulkOperation(update, Some(index, tpe), docUpdate2, retryOnConflictOpt = Some(5), upsertOpt = Some(docUpsert2))

      val updateFuture = restClient.bulkIndex(Bulk(Seq(bulkOperation1, bulkOperation2)))

      whenReady(updateFuture) { _ => refresh() }

      val resFut = restClient.query(index, tpe, new QueryRoot(TermQuery("text", "update")))

      // doc 1 should do partial update, doc 2 should do whole doc upsert
      whenReady(resFut) { res =>
        res.sourceAsMap should equal(Seq(
          Map("text" -> "update", "foo" -> "bar"),
          Map("text" -> "update", "foo" -> "bar")))
      }
    }

    "Return error when update a document that does not exist and doc as upsert is set to false" in {
      val docNotExist = Document("doc_not_exist", Map("foo" -> "bar"))
      val updateOperation = BulkOperation(update, Some(index, tpe), docNotExist, retryOnConflictOpt = Some(5), docAsUpsertOpt = Some(false))
      val resultFuture = restClient.bulkIndex(Bulk(Seq(updateOperation)))
      resultFuture.futureValue.head.status should be(404)
      resultFuture.futureValue.head.success should be(false)
    }

    "Return success when update a document that does not exist and doc as upsert is set to true" in {
      val docNotExist = Document("doc_not_exist", Map("foo" -> "bar"))
      val updateOperation = BulkOperation(update, Some(index, tpe), docNotExist, retryOnConflictOpt = Some(5), docAsUpsertOpt = Some(true) )
      val resultFuture = restClient.bulkIndex(Bulk(Seq(updateOperation)))
      resultFuture.futureValue.head.status should be(201)
      resultFuture.futureValue.head.success should be(true)
    }

    "Support case insensitive autocomplete" in {
      val basicFieldMapping = BasicFieldMapping(textType, None, Some(analyzerName))
      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("name" -> basicFieldMapping, "f1" -> basicFieldMapping, "suggest" -> CompletionMapping(Map("f" -> CompletionContext("name")), analyzerName)), Some(false)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }

      // TODO: case classes + toJson should be used rather than such a low-level approach
      val contextKey = restClient.version match {
        case V2 => "context"
        case V6 => "contexts"
      }
      val contexts = List("Case1", "case2")
      val input1 = Map(
        "name" -> "test",
        "suggest" -> Map(
          contextKey -> Map("f" -> contexts),
          "input" -> List("class")
        )
      )

      val input2 = Map(
        "name" -> "test",
        "suggest" -> Map(
          contextKey -> Map("f" -> contexts),
          "input" -> List("Case", "#Case`case")
        )
      )

      val docFut1 = restClient.index(index, tpe, Document("autocomplete1", input1))
      whenReady(docFut1) { _ => refresh() }
      val docFut2 = restClient.index(index, tpe, Document("autocomplete2", input2))
      whenReady(docFut2) { _ => refresh() }
      val lowerCaseSuggestionOpt = Some("c")
      val upperCaseSuggestionOpt = Some("C")
      val specialCharCaseSuggestionOpt = Some("#")
      val defaultWithTextSuggestion = Suggestion("my-suggestions", lowerCaseSuggestionOpt, None, Some(Completion("suggest", 50, "f", List(Context(List("Case1"))))))
      val defaulNoTextSuggestion = Suggestion("my-suggestions",None, None, Some(Completion("suggest", 50, "f", List(Context(List("Case1"))))))
      val defaultNoContextSuggestion = Suggestion("my-suggestions",None, Some(SuggestionTerm("suggest")), None)

      val suggestionWithText1 = defaultWithTextSuggestion
      val suggestionWithText2 = defaultWithTextSuggestion.copy(textOpt = upperCaseSuggestionOpt)
      val suggestionWithText3 = defaultWithTextSuggestion.copy(textOpt = specialCharCaseSuggestionOpt)

      val suggestionRootWithResult = Map(
        SuggestRoot(None, List(suggestionWithText1)) -> Set("Case", "class"),
        SuggestRoot(None, List(suggestionWithText2)) -> Set("Case", "class"),
        SuggestRoot(None, List(suggestionWithText3)) -> Set("#Case`case"),
        SuggestRoot(lowerCaseSuggestionOpt, List(defaulNoTextSuggestion)) -> Set("Case", "class"),
        SuggestRoot(specialCharCaseSuggestionOpt, List(defaulNoTextSuggestion)) -> Set("Case", "class"),
        SuggestRoot(specialCharCaseSuggestionOpt, List(defaulNoTextSuggestion)) -> Set("#Case`case")
      )

      val noContextTests = restClient.version match {
        case V6 =>
          Map(
            SuggestRoot(lowerCaseSuggestionOpt, List(defaultNoContextSuggestion)) -> Set("Case", "class"),
            SuggestRoot(specialCharCaseSuggestionOpt, List(defaultNoContextSuggestion)) -> Set("Case", "class"),
            SuggestRoot(specialCharCaseSuggestionOpt, List(defaultNoContextSuggestion)) -> Set("#Case`case"))
        case V2 =>
          // These suggest queries don't seem to be supported correctly in ES 2.3.
          Map.empty[SuggestRoot, Set[String]]
      }

      (suggestionRootWithResult ++ noContextTests).foreach {
        case (suggestionRoot, suggestionResult) => {
          withClue(suggestionRoot) {
            val result = restClient.suggest(index, tpe, suggestionRoot)
            whenReady(result) {
              resp =>
                withClue(s"Suggest root $suggestionRoot, result = ${resp.head._2.toSet}") {
                  resp.head._2.toSet should be(suggestionResult)
                }
            }
          }
        }
      }
    }

    "throw exception if both termOpt and completionOpt are defined" in {
      val suggestion = Suggestion("my-suggestions", Some("c"), Some(SuggestionTerm("suggest")), Some(Completion("suggest", 50, "f", List(Context(List("Case1"))))))
      a[IllegalArgumentException] shouldBe thrownBy{
        restClient.suggest(index, tpe, SuggestRoot(None, List(suggestion)))
      }
    }

    "Support case insensitive query" in {
      val docLower = Document("caseinsensitivequerylower", Map("f1" -> "CaSe", "f2" -> 5))
      val futLower = restClient.index(index, tpe, docLower)
      whenReady(futLower) { _ => refresh() }
      // WildcardQuery is not analyzed https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
      val resFutLower = restClient.query(index, tpe, new QueryRoot(WildcardQuery("f1", "case")))
      whenReady(resFutLower) { resp =>
        resp.extractSource[DocType].head should be(DocType("CaSe", 5))
      }
    }

    "Support range queries" in {
      val rangeFutures = (1 to 10).map { n =>
        Document(s"range-$n", Map("range-id" -> n))
      }.map { doc =>
        restClient.index(index, tpe, doc)
      }

      val range = Future.sequence(rangeFutures)
      whenReady(range) { _ =>
        refresh()
      }

      val ltQuery = new QueryRoot(RangeQuery("range-id", Lt("4")))
      val ltFut = restClient.query(index, tpe, ltQuery)
      whenReady(ltFut) { resp =>
        resp should have length 3
      }

      val lteQuery = new QueryRoot(RangeQuery("range-id", Lte("4")))
      val lteFut = restClient.query(index, tpe, lteQuery)
      whenReady(lteFut) { resp =>
        resp should have length 4
      }

      val gtQuery = new QueryRoot(RangeQuery("range-id", Gt("4")))
      val gtFut = restClient.query(index, tpe, gtQuery)
      whenReady(gtFut) { resp =>
        resp should have length 6
      }

      val gteQuery = new QueryRoot(RangeQuery("range-id", Gte("4")))
      val gteFut = restClient.query(index, tpe, gteQuery)
      whenReady(gteFut) { resp =>
        resp should have length 7
      }

      val sliceQuery = new QueryRoot(RangeQuery("range-id", Gte("5"), Lte("6")))
      val sliceFut = restClient.query(index, tpe, sliceQuery)
      whenReady(sliceFut) { resp =>
        resp should have length 2
      }
    }

    "Support multi-term filtered query" in {
      val ir = for {
        ir <- restClient.index(index, tpe, Document("multi-term-query-doc",
          Map("filter1" -> "val1", "filter2" -> "val2")))
      } yield {
        ir
      }
      whenReady(ir) { ir =>
        ir.isSuccess should be(true)
      }
      refresh()
      val termf1 = TermFilter("filter1", "val1")
      val termf2 = TermFilter("filter2", "val2")
      val termf3 = TermFilter("filter1", "val2")
      val validQuery = Bool(List(Must(MatchAll)), FilteredContext(List(termf1, termf2)))
      val invalidQuery = Bool(List(Must(MatchAll)), FilteredContext(List(termf1, termf3)))
      val invalidQuery2 = Bool(List(Must(MatchAll)), FilteredContext(List(termf3, termf1)))
      val resFut = restClient.query(index, tpe, new QueryRoot(validQuery))
      whenReady(resFut) { res =>
        res.sourceAsMap.toList should be(List(Map("filter1" -> "val1", "filter2" -> "val2")))
      }
      val resFut2 = restClient.query(index, tpe, new QueryRoot(invalidQuery))
      whenReady(resFut2) { res =>
        res.sourceAsMap.toList should be(List())
      }
      val resFut3 = restClient.query(index, tpe, new QueryRoot(invalidQuery2))
      whenReady(resFut3) { res =>
        res.sourceAsMap.toList should be(List())
      }
    }

    "Support RangeQueries along filters" in {
      val rangeFutures = (1 to 10).map { n =>
        Document(s"range-$n", Map("mod3Filter" -> s"rem${n % 3}", "range-id" -> n))
      }.map { doc =>
        restClient.index(index, tpe, doc)
      }

      val range = Future.sequence(rangeFutures)
      whenReady(range) { _ =>
        refresh()
      }
      refresh()
      val termf0 = TermFilter("mod3Filter", "rem0")
      val termf1 = TermFilter("mod3Filter", "rem1")
      val termf2 = TermFilter("mod3Filter", "rem2")
      val rangeFilterGt1 = RangeFilter("range-id", Gt("1"))
      val rangeFilterLt4 = RangeFilter("range-id", Lt("4"))
      val rangeFilterLt5 = RangeFilter("range-id", Lt("5"))

      val validQuery1 = Bool(List(Must(MatchAll)), FilteredContext(List(termf0, rangeFilterGt1)))
      val validQuery2 = Bool(List(Must(MatchAll)), FilteredContext(List(termf1, rangeFilterLt4)))
      val validQuery3 = Bool(List(Must(MatchAll)), FilteredContext(List(termf1, rangeFilterLt5)))
      val validQuery4 = Bool(List(Must(MatchAll)), FilteredContext(List(termf2, rangeFilterLt5)))
      val expectedEmptyResultQuery = Bool(List(Must(MatchAll)), FilteredContext(List(termf2, termf0, rangeFilterGt1)))

      val sortByRange = (first: Map[String, Any], second: Map[String, Any]) => {
        first("range-id").toString > second("range-id").toString
      }

      val resFut = restClient.query(index, tpe, new QueryRoot(validQuery1))
      whenReady(resFut) { res =>
        res.sourceAsMap.toList.sortWith(sortByRange) should be(
          List(
            Map("mod3Filter" -> "rem0", "range-id" -> 3),
            Map("mod3Filter" -> "rem0", "range-id" -> 6),
            Map("mod3Filter" -> "rem0", "range-id" -> 9)
          ).sortWith(sortByRange)
        )
      }

      val resFut2 = restClient.query(index, tpe, new QueryRoot(validQuery2))
      whenReady(resFut2) { res =>
        res.sourceAsMap.toList.sortWith(sortByRange) should be(
          List(
            Map("mod3Filter" -> "rem1", "range-id" -> 1)
          ).sortWith(sortByRange)
        )
      }

      val resFut3 = restClient.query(index, tpe, new QueryRoot(validQuery3))
      whenReady(resFut3) { res =>
        res.sourceAsMap.toList.sortWith(sortByRange) should be(
          List(
            Map("mod3Filter" -> "rem1", "range-id" -> 1),
            Map("mod3Filter" -> "rem1", "range-id" -> 4)
          ).sortWith(sortByRange)
        )
      }

      val resFut4 = restClient.query(index, tpe, new QueryRoot(validQuery4))
      whenReady(resFut4) { res =>
        res.sourceAsMap.toList.sortWith(sortByRange) should be(
          List(
            Map("mod3Filter" -> "rem2", "range-id" -> 2)
          ).sortWith(sortByRange)
        )
      }

      val resFutE = restClient.query(index, tpe, new QueryRoot(expectedEmptyResultQuery))
      whenReady(resFutE) { res =>
        res.sourceAsMap.toList should be(List())
      }
    }

    "Support Bool's Must and MustNot query" in {
      val mustNotDoc = Document("mustNotDoc", Map("f1" -> "MustNot", "f2" -> 5))
      val mustNotInsertionFuture = restClient.index(index, tpe, mustNotDoc)
      whenReady(mustNotInsertionFuture) { _ => refresh() }

      val mustResultFuture = restClient.query(index, tpe, new QueryRoot(Bool(List(Must(MatchQuery("f1", "MustNot"))))))
      whenReady(mustResultFuture) { resp =>
        resp.extractSource[DocType].head should be(DocType("MustNot", 5))
      }

      val mustNotResultFuture = restClient.query(index, tpe, new QueryRoot(Bool(List(MustNot(MatchQuery("f1", "MustNot"))))))
      whenReady(mustNotResultFuture) { resp =>
        resp.sourceAsMap.exists(_.get("f1").contains("MustNot")) should be(false)
      }
    }

    "Support MatchQuery" in {
      val matchDoc = Document("matchDoc", Map("f1" -> "MatchQuery", "f2" -> 5))
      val matchNotInsertionFuture = restClient.index(index, tpe, matchDoc)
      whenReady(matchNotInsertionFuture) { _ => refresh() }

      val matchResultFuture = restClient.query(index, tpe, new QueryRoot(MatchQuery("f1", "MatchQuery")))
      whenReady(matchResultFuture) { resp =>
        resp.extractSource[DocType].head should be(DocType("MatchQuery", 5))
      }
    }

    "Support MatchQuery with boost" in {
      val matchDoc = Document("matchDoc", Map("f1" -> "MatchQueryWithBoost", "f2" -> 5))
      val matchNotInsertionFuture = restClient.index(index, tpe, matchDoc)
      whenReady(matchNotInsertionFuture) { _ => refresh() }

      val matchResultFuture = restClient.query(index, tpe, new QueryRoot(MatchQuery("f1", "MatchQueryWithBoost", 5)))
      whenReady(matchResultFuture) { resp =>
        resp.extractSource[DocType].head should be(DocType("MatchQueryWithBoost", 5))
      }
    }

    "Support PhraseQuery" in {
      val phraseDoc = Document("matchDoc", Map("f1" -> "Phrase Query", "f2" -> 5))
      val phraseNotInsertionFuture = restClient.index(index, tpe, phraseDoc)
      whenReady(phraseNotInsertionFuture) { _ => refresh() }

      val phraseResultFuture = restClient.query(index, tpe, new QueryRoot(PhraseQuery("f1", "Phrase Query")))
      whenReady(phraseResultFuture) { resp =>
        resp.extractSource[DocType].head should be(DocType("Phrase Query", 5))
      }
    }

    "Support PrefixQuery" in {
      val prefixDoc = Document("prefixDoc", Map("f1" -> "foo", "f2" -> 1))
      val indexFuture = restClient.index(index, tpe, prefixDoc)
      whenReady(indexFuture) { _ => refresh() }

      val prefixQuery1 = PrefixQuery("f1", "fo")
      val prefixQuery2 = PrefixQuery("f1", "fa")

      val resultFuture1 = restClient.query(index, tpe, new QueryRoot(prefixQuery1))
      resultFuture1.futureValue.extractSource[DocType] should be(List(DocType("foo", 1)))

      val resultFuture2 = restClient.query(index, tpe, new QueryRoot(prefixQuery2))
      resultFuture2.futureValue.extractSource[DocType] should be(List())
    }

    "Support Terms Aggregation Query" in {

      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("f1" -> basicKeywordFieldMapping, "f2" -> basicNumericFieldMapping, "text" -> basicTextFieldMapping)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }

      val aggrDoc1 = Document("aggrDoc1", Map("f1" -> "aggr1", "f2" -> 1, "text" -> "text1"))
      val aggrDoc2 = Document("aggrDoc2", Map("f1" -> "aggr2", "f2" -> 2, "text" -> "text2"))
      val aggrDoc3 = Document("aggrDoc3", Map("f1" -> "aggr3", "f2" -> 1, "text" -> "text1"))
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


    "Support Terms Aggregation Query with sort order" in {
      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("f1" -> basicKeywordFieldMapping, "f2" -> basicNumericFieldMapping, "text" -> basicTextFieldMapping)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }

      val docs = Seq(
        Document("sortOrderAggrDoc1", Map("f1" -> "sortOrderAggr1", "f2" -> 1, "text" -> "text1")),
        Document("sortOrderAggrDoc2", Map("f1" -> "sortOrderAggr2", "f2" -> 2, "text" -> "text2")),
        Document("sortOrderAggrDoc3", Map("f1" -> "sortOrderAggr3", "f2" -> 1, "text" -> "text1"))
      )
      val bulkIndexFuture = restClient.bulkIndex(index, tpe, docs)
      whenReady(bulkIndexFuture) { _ => refresh() }

      val phasePrefixQuery = PrefixQuery("f1", "sortOrderAggr") // filters out ES docs for other tests
      val filteredQuery = Bool(List(Must(phasePrefixQuery)), FilteredContext(List(TermFilter("f2", "1"))))
      val termsAggr = TermsAggregation("f1", None, Some(5), Some(5), None, None, None, Some(DescSortOrder))
      val aggrQuery = AggregationQuery(filteredQuery, termsAggr, Some(1000))

      val expected = BucketAggregationResultBody(0, 0, List(Bucket("sortOrderAggr3", 1), Bucket("sortOrderAggr1", 1)))

      val aggrQueryFuture = restClient.bucketAggregation(index, tpe, aggrQuery)
      aggrQueryFuture.futureValue should be(expected)
    }


    "Support Top Hits Aggregation Query" in {
      val basicNumericFieldMapping = BasicFieldMapping(IntegerType, None, None, None, None)

      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("f1" -> basicKeywordFieldMapping, "f2" -> basicNumericFieldMapping, "text" -> basicKeywordFieldMapping)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }

      val docs = Seq(
        Document("topHitsAggrDoc1", Map("f1" -> "topHitsAggr1", "f2" -> 1, "text" -> "text1")),
        Document("topHitsAggrDoc2", Map("f1" -> "topHitsAggr2", "f2" -> 1, "text" -> "text2")),
        Document("topHitsAggrDoc3", Map("f1" -> "topHitsAggr3", "f2" -> 1, "text" -> "text1"))
      )
      val bulkIndexFuture = restClient.bulkIndex(index, tpe, docs)
      whenReady(bulkIndexFuture) { _ => refresh() }

      val phasePrefixQuery = PrefixQuery("f1", "topHitsAggr")

      val termsAggr = TermsAggregation("text", None, Some(5), Some(5), None, None,
        Some(TopHitsAggregation("thereCanBeOnlyOne", Some(1),
          Some(Seq(
            "f1",
            "text")
          ),
          Some(Map("f1" -> DescSortOrder)
          )
        )),
        Some(AscSortOrder)
      )

      val aggrQuery = AggregationQuery(phasePrefixQuery, termsAggr, Some(1000))

      val expected = Seq(Map("f1" -> "topHitsAggr3", "text" -> "text1"), Map("f1" -> "topHitsAggr2", "text" -> "text2"))

      val aggrQueryFuture = restClient.bucketNestedAggregation(index, tpe, aggrQuery)
      val aggrResult = aggrQueryFuture.futureValue
      val actual = aggrResult.underlying("buckets").
        asInstanceOf[Seq[Map[String, Any]]].
        flatMap(_ ("thereCanBeOnlyOne").
          asInstanceOf[Map[String, Any]]("hits").
          asInstanceOf[Map[String, Any]]("hits").
          asInstanceOf[Seq[Map[String, Any]]].map(_ ("_source"))
        )
      actual should be(expected)
    }


    "Support query with source filtering" in {
      val filterDoc1 = Document("filterDoc", Map("f1" -> "filter1", "f2" -> 1, "text" -> "text1"))
      val intexFuture = restClient.index(index, tpe, filterDoc1)
      whenReady(intexFuture) { _ => refresh() }
      val filterFuture = restClient.query(index, tpe, new QueryRoot(TermQuery("f1", "filter1"), sourceFilterOpt = Some(Seq("f2", "text"))))
      filterFuture.futureValue.sourceAsMap should be(List(Map("f2" -> 1, "text" -> "text1")))
    }

    "support regex query" in {
      val regexDoc1 = Document("regexQueryDoc1", Map("f1" -> "regexQuery1", "f2" -> 1, "text" -> "text1"))
      val regexDoc2 = Document("regexQueryDoc2", Map("f1" -> "regexQuery2", "f2" -> 1, "text" -> "text2"))
      val regexFuture = restClient.bulkIndex(index, tpe, Seq(regexDoc1, regexDoc2))
      whenReady(regexFuture) { _ => refresh() }

      val regexQueryFuture = restClient.query(index, tpe, new QueryRoot(RegexQuery("f1", "regexq.*1")))
      regexQueryFuture.futureValue.sourceAsMap should be(List(Map("f1" -> "regexQuery1", "f2" -> 1, "text" -> "text1")))

      val regexQueryFuture2 = restClient.query(index, tpe, new QueryRoot(RegexQuery("f1", "regexq.*")))
      regexQueryFuture2.futureValue.sourceAsMap.toSet should be(Set(Map("f1" -> "regexQuery1", "f2" -> 1, "text" -> "text1"), Map("f1" -> "regexQuery2", "f2" -> 1, "text" -> "text2")))
    }

    "support regex filter" in {
      val regexDoc1 = Document("regexFilterDoc1", Map("f1" -> "regexFilter1", "f2" -> 1, "text" -> "text1"))
      val regexDoc2 = Document("regexFilterfDoc2", Map("f1" -> "regexFilter2", "f2" -> 1, "text" -> "text2"))
      val regexFuture = restClient.bulkIndex(index, tpe, Seq(regexDoc1, regexDoc2))
      whenReady(regexFuture) { _ => refresh() }

      val filteredQuery = Bool(List(Must(MatchAll)), FilteredContext(List(RegexFilter("f1", "regexf.*1"))))
      val regexQueryFuture = restClient.query(index, tpe, new QueryRoot(filteredQuery))
      regexQueryFuture.futureValue.sourceAsMap should be(List(Map("f1" -> "regexFilter1", "f2" -> 1, "text" -> "text1")))

      val filteredQuery2 = Bool(List(Must(MatchAll)), FilteredContext(List(RegexFilter("f1", "regexf.*"))))
      val regexQueryFuture2 = restClient.query(index, tpe, new QueryRoot(filteredQuery2))
      regexQueryFuture2.futureValue.sourceAsMap.toSet should be(Set(Map("f1" -> "regexFilter1", "f2" -> 1, "text" -> "text1"), Map("f1" -> "regexFilter2", "f2" -> 1, "text" -> "text2")))
    }

    "support buckets inside buckets" in {
      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("make" -> basicKeywordFieldMapping, "color" -> basicKeywordFieldMapping), Some(false)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }


      // https://www.elastic.co/guide/en/elasticsearch/guide/current/_buckets_inside_buckets.html
      val doc1 = Document("agg_doc1", Map("make" -> "honda", "color" -> "red"))
      val doc2 = Document("agg_doc2", Map("make" -> "honda", "color" -> "black"))
      val doc3 = Document("agg_doc3", Map("make" -> "honda", "color" -> "black"))

      val regexFuture = restClient.bulkIndex(index, tpe, Seq(doc1, doc2, doc3))
      whenReady(regexFuture) { _ => refresh() }

      val aggregations = TermsAggregation(name = Some("make"), field = "make", include = None, size = Some(Int.MaxValue), shardSize = None, hint = None,
        aggs = Some(TermsAggregation(name = Some("color"), field = "color", include = None, size = Some(Int.MaxValue), shardSize = None, hint = None))
      )

      val aggregationsQuery = AggregationQuery(
        query = MatchAll,
        aggs = aggregations,
        timeout = None
      )

      val expected = BucketNested(Map("doc_count_error_upper_bound" -> 0, "sum_other_doc_count" -> 0, "buckets" -> List(Map("key" -> "honda", "doc_count" -> 3, "color" -> Map("doc_count_error_upper_bound" -> 0, "sum_other_doc_count" -> 0, "buckets" -> List(Map("key" -> "black", "doc_count" -> 2), Map("key" -> "red", "doc_count" -> 1)))))))
      val aggregationsQueryFuture = restClient.bucketNestedAggregation(index, tpe, aggregationsQuery)
      aggregationsQueryFuture.futureValue should be(expected)
    }

    "Support NestedQuery" in {
      // https://www.elastic.co/guide/en/elasticsearch/reference/2.3/query-dsl-nested-query.html
      val metadataMapping = Mapping(tpe, IndexMapping(Map("user" -> NestedFieldMapping), None))
      val mappingFuture = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFuture) { _ => refresh() }
      val userDoc = List(Map("first" -> "john", "last" -> "Smith"), Map("first" -> "Alice", "last" -> "White"))
      val matchDoc = Document("matchDoc", Map("user" -> userDoc))
      val matchDocInsertionFuture = restClient.index(index, tpe, matchDoc)
      whenReady(matchDocInsertionFuture) { _ => refresh() }

      val matchResultFuture = restClient.query(index, tpe, new QueryRoot(NestedQuery("user", Some(AvgScoreMode), Bool(List(Must(MatchQuery("user.first", "Alice")))))))
      whenReady(matchResultFuture) { resp =>
        resp.extractSource[DocNestedType].head should be(DocNestedType(userDoc))
      }
    }

    "support multi match query" in {
      // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html
      val multiMatchDoc1 = Document("multiMatchDoc1", Map("f1" -> "multimatch1", "f2" -> 1, "text" -> "text1"))
      val multiMatchDoc2 = Document("multiMatchDoc2", Map("f1" -> "text1", "f2" -> 1, "text" -> "multimatch1"))
      val docsFuture = restClient.bulkIndex(index, tpe, Seq(multiMatchDoc1, multiMatchDoc2))
      whenReady(docsFuture) { _ => refresh() }

      val matchQuery = MultiMatchQuery("multimatch1", Map(), "f1", "text")
      val matchQueryFuture = restClient.query(index, tpe, new QueryRoot(matchQuery))
      matchQueryFuture.futureValue.sourceAsMap.toSet should be(Set(Map("f1" -> "text1", "f2" -> 1, "text" -> "multimatch1"), Map("f1" -> "multimatch1", "f2" -> 1, "text" -> "text1")))
    }

    "support multi match query with option" in {
      // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html
      val multiMatchDoc1 = Document("multiMatchOptionDoc1", Map("f1" -> "multimatch1 test", "f2" -> 1, "text" -> "text1"))
      val multiMatchDoc2 = Document("multiMatchOptionDoc2", Map("f1" -> "text1", "f2" -> 1, "text" -> "multimatch1"))
      val docsFuture = restClient.bulkIndex(index, tpe, Seq(multiMatchDoc1, multiMatchDoc2))
      whenReady(docsFuture) { _ => refresh() }

      val matchQuery = MultiMatchQuery("multimatch1 test", Map("operator" -> "and"), "f1", "text")
      val matchQueryFuture = restClient.query(index, tpe, new QueryRoot(matchQuery))
      matchQueryFuture.futureValue.sourceAsMap.toSet should be(Set(Map("f1" -> "multimatch1 test", "f2" -> 1, "text" -> "text1")))
    }

    "support disjunction max query" in {
      // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-dis-max-query.html
      val disMaxDoc1 = Document("disMaxDoc1", Map("title" -> "Quick brown rabbits", "body" -> "Brown rabbits are commonly seen."))
      val disMaxDoc2 = Document("disMaxDoc2", Map("title" -> "Keeping pets healthy", "body" -> "My quick brown fox eats rabbits on a regular basis."))
      val docsFuture = restClient.bulkIndex(index, tpe, Seq(disMaxDoc1, disMaxDoc2))
      whenReady(docsFuture) { _ => refresh() }

      val disMaxQuery = DisMaxQuery(queries = Seq(MatchQuery("title", "Brown fox"), MatchQuery("body", "Brown fox")))
      val disMaxQueryFuture = restClient.query(index, tpe, new QueryRoot(disMaxQuery))
      disMaxQueryFuture.futureValue.sourceAsMap should be(
        Seq(
          Map("title" -> "Keeping pets healthy", "body" -> "My quick brown fox eats rabbits on a regular basis."),
          Map("title" -> "Quick brown rabbits", "body" -> "Brown rabbits are commonly seen.")
        )
      )

      val disMaxQuery2 = DisMaxQuery(
        queries = Seq(MatchQuery("title", "Brown fox"), MatchQuery("body", "Brown fox")), Some(0.3f), Some(1.0f))
      val disMaxQueryFuture2 = restClient.query(index, tpe, new QueryRoot(disMaxQuery2))
      disMaxQueryFuture2.futureValue.sourceAsMap.toSet should be(
        Set(
          Map("title" -> "Keeping pets healthy", "body" -> "My quick brown fox eats rabbits on a regular basis."),
          Map("title" -> "Quick brown rabbits", "body" -> "Brown rabbits are commonly seen.")
        )
      )
    }

    "support geo distance filter" in {
      // https://www.elastic.co/guide/en/elasticsearch/guide/current/geo-distance.html
      val geoPointMapping = BasicFieldMapping(GeoPointType, None, None)
      val metadataMapping = Mapping(tpe, IndexMapping(Map("location" -> geoPointMapping), Some(false)))
      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }

      val locationDoc1 = Document("locationDoc1", Map("category" -> "categoryName", "location" -> "40.715, -74.011"))
      val locationDoc2 = Document("locationDoc2", Map("category" -> "categoryName", "location" -> "1, 1"))
      val locDocsFuture = restClient.bulkIndex(index, tpe, Seq(locationDoc1, locationDoc2))
      whenReady(locDocsFuture) { _ => refresh() }

      val geoQuery = Bool(
        List(Must(MatchQuery("category", "categoryName"))),
        FilteredContext(List(GeoDistanceFilter(s"1km", "location", GeoLocation(40.715, -74.011)))))

      val geoQueryFuture = restClient.query(index, tpe, new QueryRoot(geoQuery))
      geoQueryFuture.futureValue.sourceAsMap.toSet should be(Set(Map("category" -> "categoryName", "location" -> "40.715, -74.011")))
    }

    "support simple sorting" in {
      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("f1" -> basicTextFieldMapping, "cat" -> basicKeywordFieldMapping)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }

      // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html
      val sortDoc1 = Document("simpleSortDoc1", Map("f1" -> "simpleSort", "cat" -> "aaa"))
      val sortDoc2 = Document("simpleSortDoc2", Map("f1" -> "simpleSort", "cat" -> "aab"))
      val sortFuture = restClient.bulkIndex(index, tpe, Seq(sortDoc1, sortDoc2))
      whenReady(sortFuture) { ok => refresh() }
      val sortQueryAscFuture = restClient.query(index, tpe, new QueryRoot(
        query = MatchQuery("f1", "simpleSort"),
        fromOpt = None,
        sizeOpt = None,
        sortOpt = Some(Seq(SimpleSort("cat", AscSortOrder))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None)
      )
      sortQueryAscFuture.futureValue.sourceAsMap should be(Seq(Map("f1" -> "simpleSort", "cat" -> "aaa"), Map("f1" -> "simpleSort", "cat" -> "aab")))

      val sortQueryDescFuture = restClient.query(index, tpe, new QueryRoot(
        query = MatchQuery("f1", "simpleSort"),
        fromOpt = None,
        sizeOpt = None,
        sortOpt = Some(Seq(SimpleSort("cat", DescSortOrder))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None)
      )
      sortQueryDescFuture.futureValue.sourceAsMap should be(Seq(Map("f1" -> "simpleSort", "cat" -> "aab"), Map("f1" -> "simpleSort", "cat" -> "aaa")))
    }

    "support multiple simple sorting" in {
      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("f1" -> basicTextFieldMapping, "cat" -> basicKeywordFieldMapping, "dog" -> basicKeywordFieldMapping)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }

      // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html
      val sortDoc1 = Document("multiSimpleSortDoc1", Map("f2" -> "multiSimpleSort", "cat" -> "aaa", "dog" -> "ccd"))
      val sortDoc2 = Document("multiSimpleSortDoc2", Map("f2" -> "multiSimpleSort", "cat" -> "aab", "dog" -> "ccd"))
      val sortDoc3 = Document("multiSimpleSortDoc3", Map("f2" -> "multiSimpleSort", "cat" -> "aab", "dog" -> "ccc"))
      val sortFuture = restClient.bulkIndex(index, tpe, Seq(sortDoc1, sortDoc2, sortDoc3))
      whenReady(sortFuture) { ok => refresh() }
      val sortQueryAscDescFuture = restClient.query(index, tpe, new QueryRoot(
        query = MatchQuery("f2", "multiSimpleSort"),
        fromOpt = None,
        sizeOpt = None,
        sortOpt = Some(Seq(SimpleSort("cat", AscSortOrder), SimpleSort("dog", DescSortOrder))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None)
      )
      sortQueryAscDescFuture.futureValue.sourceAsMap should be(Seq(
        Map("f2" -> "multiSimpleSort", "cat" -> "aaa", "dog" -> "ccd"),
        Map("f2" -> "multiSimpleSort", "cat" -> "aab", "dog" -> "ccd"),
        Map("f2" -> "multiSimpleSort", "cat" -> "aab", "dog" -> "ccc"))
      )

      val sortQueryAscAscFuture = restClient.query(index, tpe, new QueryRoot(
        query = MatchQuery("f2", "multiSimpleSort"),
        fromOpt = None,
        sizeOpt = None,
        sortOpt = Some(Seq(SimpleSort("cat", AscSortOrder), SimpleSort("dog", AscSortOrder))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None)
      )
      sortQueryAscAscFuture.futureValue.sourceAsMap should be(Seq(
        Map("f2" -> "multiSimpleSort", "cat" -> "aaa", "dog" -> "ccd"),
        Map("f2" -> "multiSimpleSort", "cat" -> "aab", "dog" -> "ccc"),
        Map("f2" -> "multiSimpleSort", "cat" -> "aab", "dog" -> "ccd"))
      )

      val sortQueryDescDescFuture = restClient.query(index, tpe, new QueryRoot(
        query = MatchQuery("f2", "multiSimpleSort"),
        fromOpt = None,
        sizeOpt = None,
        sortOpt = Some(Seq(SimpleSort("cat", DescSortOrder), SimpleSort("dog", DescSortOrder))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None)
      )
      sortQueryDescDescFuture.futureValue.sourceAsMap should be(Seq(
        Map("f2" -> "multiSimpleSort", "cat" -> "aab", "dog" -> "ccd"),
        Map("f2" -> "multiSimpleSort", "cat" -> "aab", "dog" -> "ccc"),
        Map("f2" -> "multiSimpleSort", "cat" -> "aaa", "dog" -> "ccd"))
      )

      val sortQueryDescAscFuture = restClient.query(index, tpe, new QueryRoot(
        query = MatchQuery("f2", "multiSimpleSort"),
        fromOpt = None,
        sizeOpt = None,
        sortOpt = Some(Seq(SimpleSort("cat", DescSortOrder), SimpleSort("dog", AscSortOrder))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None)
      )
      sortQueryDescAscFuture.futureValue.sourceAsMap should be(Seq(
        Map("f2" -> "multiSimpleSort", "cat" -> "aab", "dog" -> "ccc"),
        Map("f2" -> "multiSimpleSort", "cat" -> "aab", "dog" -> "ccd"),
        Map("f2" -> "multiSimpleSort", "cat" -> "aaa", "dog" -> "ccd"))
      )
    }

    "support sorting by Distance" in {
      // https://www.elastic.co/guide/en/elasticsearch/guide/current/sorting-by-distance.html
      val geoPointMapping = BasicFieldMapping(GeoPointType, None, None)
      val metadataMapping = Mapping(tpe, IndexMapping(Map("location" -> geoPointMapping), Some(false)))
      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }
      val locationDoc1 = Document("distanceSortDoc1", Map("f1" -> "distanceSortDoc", "location" -> "40.715, -74.011"))
      val locationDoc2 = Document("distanceSortDoc2", Map("f1" -> "distanceSortDoc", "location" -> "1, 1"))
      val locDocsFuture = restClient.bulkIndex(index, tpe, Seq(locationDoc1, locationDoc2))
      whenReady(locDocsFuture) { _ => refresh() }

      val sortQueryAscFuture = restClient.query(index, tpe, new QueryRoot(
        MatchQuery("f1", "distanceSortDoc"),
        fromOpt = None,
        sizeOpt = None,
        sortOpt = Some(Seq(GeoDistanceSort("location", GeoLocation(40.715, -74.011), AscSortOrder, "km", "plane"))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None)
      )
      sortQueryAscFuture.futureValue.sourceAsMap should be(Seq(Map("f1" -> "distanceSortDoc", "location" -> "40.715, -74.011"), Map("f1" -> "distanceSortDoc", "location" -> "1, 1")))

      val sortQueryDescFuture = restClient.query(index, tpe, new QueryRoot(
        MatchQuery("f1", "distanceSortDoc"),
        fromOpt = None,
        sizeOpt = None,
        sortOpt = Some(Seq(GeoDistanceSort("location", GeoLocation(40.715, -74.011), DescSortOrder, "km", "plane"))),
        timeoutOpt = None,
        sourceFilterOpt = None,
        terminateAfterOpt = None)
      )
      sortQueryDescFuture.futureValue.sourceAsMap should be(Seq(Map("f1" -> "distanceSortDoc", "location" -> "1, 1"), Map("f1" -> "distanceSortDoc", "location" -> "40.715, -74.011")))
    }

    "not return rawJsonStr if not required" in {
      indexDocs(Seq(Document("doc1", Map("text" -> "here"))))
      val resFut = restClient.query(index, tpe, new QueryRoot(TermQuery("text", "here")), rawJsonStr = false)
      resFut.futureValue.jsonStr should be("")
      resFut.futureValue.sourceAsMap.head should be(Map("text" -> "here"))
    }

    "support query with highlights with extended query root" in {
      val basicFieldFreqsMapping = BasicFieldMapping(textType, None, Some(analyzerName), None,
        Some(analyzerName), indexOption = Some(FreqsIndexOption))
      val basicFieldOffsetsMapping = BasicFieldMapping(textType, None, Some(analyzerName), None,
        Some(analyzerName), indexOption = Some(OffsetsIndexOption))

      val metadataMapping = Mapping(tpe, IndexMapping(
        Map("f1" -> basicFieldFreqsMapping, "text" -> basicFieldOffsetsMapping)))

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }
      indexDocs(Seq(Document("doc1", Map("f1" -> "text", "text" -> "here"))))
      val resp = restClient.query(index, tpe, new QueryRoot(PrefixQuery("text", "h"))).futureValue
      resp.sourceAsMap should not be empty
      val highlights = Highlight(Seq(HighlightField("text", Some(PlainHighlighter), None, Some(0)),
        HighlightField("f1", Some(PlainHighlighter))), Seq(""), Seq(""))
      val resFut = restClient.query(index, tpe,
        HighlightRoot(new QueryRoot(
          PrefixQuery("text", "h"),
          fromOpt = None,
          sizeOpt = None,
          sortOpt = None,
          timeoutOpt = None,
          sourceFilterOpt = Some(Seq("false")),
          terminateAfterOpt = Some(10)),
          highlights))
      resFut.futureValue.rawSearchResponse.highlightAsMaps.head should be(Map("text" -> List("here")))
    }

    "support check where a document exists by document id" in {
      val doc = Document("doc0001", Map("text" -> "here"))
      val indexFuture = restClient.bulkIndex(index, tpe, Seq(doc))
      whenReady(indexFuture) { _ => refresh() }
      restClient.documentExistsById(index, tpe, "doc0001").futureValue should be(true)
      restClient.documentExistsById(index, tpe, "doc0002").futureValue should be(false)
    }

    "Support deleting more than 10000 docs" in {
      val docsCount = 10011
      val documents = (1 to docsCount).map(i => Document(s"doc$i", Map("text7" -> "here7")))
      val bulkInsertResult = restClient.bulkIndex(index, tpe,documents)
      Await.result(bulkInsertResult, 20.seconds)
      refresh()

      val count = Await.result(restClient.count(index, tpe, new QueryRoot(MatchAll)), 10.seconds)
      count should be(docsCount)

      val delFut = restClient.deleteDocuments(index, tpe, new QueryRoot(MatchAll, sizeOpt = Some(1000)))
      Await.result(delFut, 20.seconds)
      refresh()

      val count1 = Await.result(restClient.count(index, tpe, new QueryRoot(MatchAll)), 10.seconds)
      count1 should be(0)
    }

    "Support deleting by a query" in {
      val docsCount = 10011
      val documents = (1 to docsCount).map(i => Document(s"doc$i", Map("text7" -> "here7")))
      val bulkInsertResult = restClient.bulkIndex(index, tpe,documents)
      Await.result(bulkInsertResult, 20.seconds)
      refresh()

      val count = Await.result(restClient.count(index, tpe, new QueryRoot(MatchAll)), 10.seconds)
      count should be(docsCount)

      val termQuery = TermQuery("text7", "here7")

      val delFut = restClient.deleteByQuery(index, tpe, new QueryRoot(termQuery), true)
      Await.result(delFut, 20.seconds)
      refresh()

      val count1 = Await.result(restClient.count(index, tpe, new QueryRoot(termQuery)), 10.seconds)
      count1 should be(0)
    }

    "Delete only first page of query results" in {
      val insertFutures = (1 to 100).map(i => restClient.index(index, tpe, Document(s"doc$i", Map("text7" -> "here7"))))
      val ir = Future.sequence(insertFutures)
      Await.result(ir, 20.seconds)
      refresh()

      val delFut = restClient.deleteDocument(index, tpe, new QueryRoot(MatchAll, sizeOpt = Some(50)))
      Await.result(delFut, 20.seconds)
      refresh()

      val count = Await.result(restClient.count(index, tpe, new QueryRoot(MatchAll)), 10.seconds)
      count should be(50)
    }

    "Support deleting a doc that doesn't exist" in {
      val delFut = restClient.deleteDocuments(index, tpe, new QueryRoot(TermQuery("text7", "here7")))
      Await.result(delFut, 10.seconds) // May not need Await?
    }

    "Support nested mapping" in {
      val basicFieldMapping = BasicFieldMapping(textType, None, Some(analyzerName), ignoreAbove = None, Some(analyzerName))
      val metadataMapping = Mapping(tpe,
        IndexMapping(
          Map("name" -> basicFieldMapping,
            "kv" -> NestedObjectMapping(Map("key" -> basicFieldMapping, "val" -> basicFieldMapping))
          )
        )
      )

      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }
      val mappingRes = restClient.getMapping(index, tpe)
      val expected = s""""kv":{"type":"nested","properties":{"key":{"type":"${textType.rep}","analyzer":"keyword_lowercase"},"val":{"type":"${textType.rep}","analyzer":"keyword_lowercase"}}}"""
      mappingRes.futureValue.jsonStr.toString.contains(expected) should be(true)
    }

    "Support multi-fields mapping" in {
      val fields = FieldsMapping(Map("raw" -> BasicFieldMapping(textType, None, Some(analyzerName), ignoreAbove = None, Some(analyzerName))))
      val basicFieldMapping = BasicFieldMapping(textType, None, Some(analyzerName), ignoreAbove = None, Some(analyzerName), fieldsOption = Some(fields))
      val metadataMapping = Mapping(tpe,
        IndexMapping(
          Map("multi-fields" -> basicFieldMapping)
        )
      )
      val mappingFut = restClient.putMapping(index, tpe, metadataMapping)
      whenReady(mappingFut) { _ => refresh() }
      val mappingRes = restClient.getMapping(index, tpe)
      val expected = s"""{"multi-fields":{"type":"${textType.rep}","fields":{"raw":{"type":"${textType.rep}","analyzer":"keyword_lowercase"}},"analyzer":"keyword_lowercase"}}"""
      mappingRes.futureValue.jsonStr.toString.contains(expected) should be(true)
    }

    "Support query profiling" in {
      indexDocs(
        Seq(
          Document("doc1", Map("animal" -> "wombat")),
          Document("doc2", Map("animal" -> "koala"))))
      val resFut = restClient.query(
        index,
        tpe,
        new QueryRoot(TermQuery("animal", "wombat"), timeoutOpt = Some(5000)),
        profile = true)

      whenReady(resFut) { res =>
        res.sourceAsMap.toList should be(List(Map("animal" -> "wombat")))
        res.rawSearchResponse.profile should not be empty
        res.rawSearchResponse.profile should contain key "shards"
      }
    }
  }
}

private case class DocNestedType(user: List[Map[String, String]])

private case class DocType(f1: String, f2: Int)
