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
package com.sumologic.elasticsearch.restlastic.dsl

import scala.collection.concurrent.TrieMap

trait IndexDsl extends DslCommons {

  case class CreateIndex(settings: Option[IndexSetting] = None) extends RootObject {
    val _settings = "settings"

    override def toJson(version: EsVersion): Map[String, Any] = settings.map { s =>
      Map(_settings -> s.toJson(version))
    }.getOrElse(Map.empty[String, Any])
  }

  case class Document(id: String, data: Map[String, Any]) extends EsOperation with RootObject {
    override def toJson(version: EsVersion): Map[String, Any] = data
  }

  // Op types are lowercased to avoid a conflict with the Index case class
  sealed trait OperationType {
    val jsonStr: String
  }

  case object index extends OperationType {
    override val jsonStr: String = "index"
  }

  case object create extends OperationType {
    override val jsonStr: String = "create"
  }

  case object update extends OperationType {
    override val jsonStr: String = "update"
  }

  case object delete extends OperationType {
    override val jsonStr: String = "delete"
  }

  case class Bulk(operations: Seq[BulkOperation]) extends EsOperation with RootObject {
    private val jsonStrCache = TrieMap.empty[EsVersion, String]

    override def toJson(version: EsVersion): Map[String, Any] = throw new UnsupportedOperationException

    override def toJsonStr(version: EsVersion): String = {
      jsonStrCache.getOrElseUpdate(version, operations.map(_.toJsonStr(version)).mkString("", "\n", "\n"))
    }
  }

  // When upsertOpt is specified, its content is used for upsert as described in
  // https://www.elastic.co/guide/en/elasticsearch/reference/2.3/docs-update.html
  // When upsertOpt is not given, document is used for upsert.
  case class BulkOperation(operation: OperationType, location: Option[(Index, Type)], document: Document, retryOnConflictOpt: Option[Int] = None, upsertOpt: Option[Document] = None, docAsUpsertOpt: Option[Boolean] = None) extends EsOperation {
    import EsOperation.compactJson

    private val jsonStrCache = TrieMap.empty[EsVersion, String]

    override def toJson(version: EsVersion): Map[String, Any] = throw new UnsupportedOperationException

    def toJsonStr(version: EsVersion): String = {
      jsonStrCache.getOrElseUpdate(version, makeJsonStr(version))
    }

    private def makeJsonStr(version: EsVersion): String = {
      val (doc, retryOpt) = operation match {
        case `update` =>
          val updateOps = upsertOpt match {
            case Some(upsert) =>
              Map("upsert" -> upsert.data)
            case None =>
              Map("doc_as_upsert" -> docAsUpsertOpt.getOrElse(true))
          }
          (Document(document.id, Map("doc" -> document.data) ++ Map("detect_noop" -> true) ++ updateOps), retryOnConflictOpt.map(n => Map("_retry_on_conflict" -> n)))
        case _ => (document, None)
      }

      val jsonObjects = Map(operation.jsonStr ->
          (Map("_id" -> doc.id) ++ location.map { case (index, tpe) => Map("_index" -> index.name, "_type" -> tpe.name) }.getOrElse(Map()) ++ retryOpt.getOrElse(Map()))
      )
      operation match {
        case `delete` => s"${compactJson(jsonObjects)}"
        case _ => s"${compactJson(jsonObjects)}\n${doc.toJsonStr(version)}"
      }
    }
  }

  case class IndexSetting(numberOfShards: Int,
                          numberOfReplicas: Int,
                          analyzerMapping: Analysis,
                          refreshInterval: Int = 1,
                          preload: Seq[String] = Seq.empty[String],
                          maxResultWindow: Int = 10000)
      extends EsOperation {

    val _shards = "number_of_shards"
    val _replicas = "number_of_replicas"
    val _analysis = "analysis"
    val _interval = "refresh_interval"
    val _preload = "index.store.preload"
    val _maxResultWindow = "max_result_window"

    override def toJson(version: EsVersion): Map[String, Any] = {
      Map(
        _shards -> numberOfShards,
        _replicas -> numberOfReplicas,
        _analysis -> analyzerMapping.toJson(version),
        _interval -> s"${refreshInterval}s",
        _maxResultWindow -> maxResultWindow) ++
        (if (preload.isEmpty) Map.empty[String, Any] else Map(_preload -> preload))
    }
  }

  sealed trait Analysis extends EsOperation

  case class Analyzers(analyzers: AnalyzerArray,
                       filters: FilterArray,
                       normalizers: NormalizerArray = NormalizerArray())
      extends Analysis with EsOperation {

    override def toJson(version: EsVersion): Map[String, Any] = {
      analyzers.toJson(version) ++ filters.toJson(version) ++ normalizers.toJson(version)
    }
  }

  object Analyzers {
    def empty: Analyzers = Analyzers(AnalyzerArray(), FilterArray())
  }

  case class Analyzer(name: Name, tokenizer: FieldType, filter: FieldType*)
      extends Analysis with EsOperation {

    val _analyzer = "analyzer"
    val _tokenizer = "tokenizer"
    val _filter = "filter"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _analyzer -> Map(
        name.name -> Map(
          _tokenizer -> tokenizer.rep,
          _filter -> filter.map(_.rep)
        )
      )
    )
  }

  case class Normalizers(normalizers: Normalizer)

  case class Normalizer(name: Name, filter: FieldType*) extends Analysis {
    val _normalizer = "normalizer"
    val _filter = "filter"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _normalizer -> Map(
        name.name -> Map(
          "type" -> "custom",
          _filter -> filter.map(_.rep)))
    )
  }

  sealed trait Filter extends EsOperation {
    val name: Name
  }

  case class EdgeNGramFilter(name: Name, minGram: Int, maxGram: Int) extends Filter with EsOperation {

    val _filter = "filter"
    val _type = "type"
    val _edgeNGram = "edge_ngram"
    val _minGram = "min_gram"
    val _maxGramp = "max_gram"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _filter -> Map(
        name.name -> Map(
          _type -> _edgeNGram,
          _minGram -> minGram,
          _maxGramp -> maxGram
        )
      )
    )
  }

  case class PatternFilter(name: Name, preserveOriginal: Boolean, patterns: Seq[String]) extends Filter with EsOperation {
    val _filter = "filter"
    val _type = "type"
    val _patternCapture = "pattern_capture"
    val _preserveOriginal = "preserve_original"
    val _patterns = "patterns"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _filter -> Map(
        name.name -> Map(
          _type -> _patternCapture,
          _preserveOriginal -> preserveOriginal,
          _patterns -> patterns
        )
      )
    )
  }

  case class AnalyzerArray(analyzers: Analyzer*) extends EsOperation {
    val _analyzer = "analyzer"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _analyzer -> analyzers.map(_
          .toJson(version).getOrElse(_analyzer, Map())
          .asInstanceOf[Map[String, Any]]).foldLeft(Map.empty[String, Any]) { _ ++ _ }
    )
  }

  case class NormalizerArray(normalizers: Normalizer*) extends EsOperation {
    val _normalizer = "normalizer"

    override def toJson(version: EsVersion): Map[String, Any] = version match {
      case V2 => Map.empty[String, Any]
      case V6 => Map(
        _normalizer -> normalizers.
          map(_.toJson(version).getOrElse(_normalizer, Map()).
            asInstanceOf[Map[String, Any]]).foldLeft(Map.empty[String, Any]) { _ ++ _ }
      )
    }
  }

  case class FilterArray(filters: Filter*) extends EsOperation {
    val _filter = "filter"

    override def toJson(version: EsVersion): Map[String, Any] = Map(
      _filter -> filters.map(_
          .toJson(version).getOrElse(_filter, Map())
          .asInstanceOf[Map[String, Any]]).foldLeft(Map.empty[String, Any])(_ ++ _)
    )
  }

  case object Keyword extends FieldType {
    val rep = "keyword"
  }

  case object Lowercase extends FieldType {
    val rep = "lowercase"
  }

  case object EdgeNGram extends FieldType {
    val rep = "edgengram"
  }

  case object PatternFilter extends FieldType {
    val rep = "pattern_filter"
  }

}


